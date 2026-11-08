//! Host mode: the invite loop, per-friend supervision and reaching the Minecraft server.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout, Instant};
use tracing::{debug, info, warn};
use webrtc::data_channel::DataChannel;

use crate::bridge::{bridge, Counter, Slot};
use crate::console::Console;
use crate::control::{self, channel_label, stream_id, ControlEnd, Role, Transport};
use crate::datagram::{self, relay, Endpoint};
use crate::edition::Edition;
use crate::events::{Event, Reporter};
use crate::invite::{sdp_fingerprints, Payload, PayloadType, DEFAULT_TTL};
use crate::peer::{ChannelHandler, Peer, PeerOptions};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const DIAL_TIMEOUT: Duration = Duration::from_secs(5);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

pub struct HostOptions {
    pub peer: PeerOptions,
    pub edition: Edition,
    pub server: String,
    pub max_peers: Option<usize>,
    pub max_connections: Option<usize>,
    pub whitelist: Option<HashSet<String>>,
    pub raw_json: bool,
    pub reporter: Reporter,
}

/// Turns the friend's `open` requests into connections to the Minecraft server.
#[derive(Clone)]
pub struct Tunnel {
    pub peer: Peer,
    pub control: Arc<dyn DataChannel>,
    pub edition: Edition,
    pub server: String,
    pub max_connections: Option<usize>,
    pub label: String,
    pub connections: Counter,
}

/// This side's connection to the Minecraft server.
enum Server {
    Tcp(TcpStream),
    Udp(UdpSocket),
}

impl Tunnel {
    pub fn role(self) -> Role {
        Role::Host(Arc::new(move |id, transport| {
            tokio::spawn(self.clone().serve(id, transport));
        }))
    }

    async fn serve(self, id: u64, transport: Transport) {
        let channel_name = channel_label(id);
        if transport == Transport::Datagrams && self.edition == Edition::Java {
            warn!(peer = %self.label, "refusing {channel_name}, Java has no datagrams");
            control::answer_open(self.control.as_ref(), id, false).await;
            return;
        }
        let Some(_slot) = self.connections.try_acquire(self.max_connections) else {
            warn!(peer = %self.label, "connection limit reached, refusing {channel_name}");
            control::answer_open(self.control.as_ref(), id, false).await;
            return;
        };
        let Some((server, channel)) = self.open(&channel_name, id, transport).await else {
            control::answer_open(self.control.as_ref(), id, false).await;
            return;
        };
        control::answer_open(self.control.as_ref(), id, true).await;
        debug!(peer = %self.label, channel = %channel_name, "connection opened");
        match server {
            Server::Tcp(stream) => bridge(stream, channel).await,
            Server::Udp(socket) => relay(Endpoint::server(socket), channel).await,
        }
        debug!(peer = %self.label, channel = %channel_name, "connection closed");
    }

    async fn dial(&self, transport: Transport) -> Option<Server> {
        let dial = async {
            match transport {
                Transport::Stream => TcpStream::connect(&self.server).await.map(Server::Tcp),
                Transport::Datagrams => datagram::dial(&self.server).await.map(Server::Udp),
            }
        };
        match timeout(DIAL_TIMEOUT, dial).await {
            Ok(Ok(server)) => Some(server),
            Ok(Err(error)) => {
                warn!(server = %self.server, %error, "could not reach the Minecraft server");
                None
            }
            Err(_) => {
                warn!(server = %self.server, "timed out dialing the Minecraft server");
                None
            }
        }
    }

    async fn open(
        &self,
        label: &str,
        id: u64,
        transport: Transport,
    ) -> Option<(Server, Arc<dyn DataChannel>)> {
        let server = self.dial(transport).await?;
        let channel = match server {
            Server::Tcp(_) => {
                self.peer
                    .create_negotiated_channel(label, stream_id(id))
                    .await
            }
            Server::Udp(_) => {
                self.peer
                    .create_datagram_channel(label, stream_id(id))
                    .await
            }
        };
        match channel {
            Ok(channel) => Some((server, channel)),
            Err(error) => {
                warn!(peer = %self.label, "{error:#}");
                None
            }
        }
    }
}

/// Friends announce connections on the control channel, so nothing else should show up here.
fn refuse_channels() -> ChannelHandler {
    Arc::new(|channel| {
        tokio::spawn(async move {
            warn!("refusing an unexpected data channel");
            let _ = channel.close().await;
        });
    })
}

/// A reply is acceptable when every fingerprint it declares is whitelisted, so a reply cannot
/// pair someone else's ID with its own certificate.
fn check_whitelist(whitelist: &HashSet<String>, sdp: &str) -> Result<()> {
    let fingerprints = sdp_fingerprints(sdp);
    let Some(unknown) = fingerprints.iter().find(|id| !whitelist.contains(*id)) else {
        if fingerprints.is_empty() {
            bail!(
                "the reply has no DTLS fingerprint, so it cannot be checked against the whitelist"
            );
        }
        return Ok(());
    };
    bail!("this friend is not on the whitelist (their ID is `{unknown}`)");
}

struct Session {
    tunnel: Tunnel,
    slot: Slot,
}

/// Reports connection counts and ping until cancelled.
pub async fn report_stats(
    label: String,
    connections: Counter,
    mut rtt: watch::Receiver<Option<Duration>>,
    reporter: Reporter,
) {
    let mut count = connections.subscribe();
    loop {
        let event = Event::Stats {
            label: label.clone(),
            connections: *count.borrow_and_update(),
            ping_ms: rtt.borrow_and_update().map(|rtt| rtt.as_millis() as u64),
        };
        reporter.report(event);
        tokio::select! {
            result = count.changed() => if result.is_err() { break },
            result = rtt.changed() => if result.is_err() { break },
        }
    }
    std::future::pending::<()>().await;
}

/// Runs an established friend until they leave.
async fn supervise(session: Session, shutdown: watch::Receiver<bool>, reporter: Reporter) {
    let Session { tunnel, slot } = session;
    let (peer, control, label, connections) = (
        tunnel.peer.clone(),
        tunnel.control.clone(),
        tunnel.label.clone(),
        tunnel.connections.clone(),
    );
    let (rtt, measured) = watch::channel(None);
    let stats = report_stats(
        label.clone(),
        connections.clone(),
        measured,
        reporter.clone(),
    );
    let reason = tokio::select! {
        end = control::run(control, tunnel.role(), shutdown, rtt) => match end {
            ControlEnd::PeerLeft => Some("left".to_owned()),
            ControlEnd::Closed => Some("disconnected".to_owned()),
            ControlEnd::TimedOut => Some("timed out".to_owned()),
            ControlEnd::Shutdown => None,
        },
        state = peer.wait_lost() => Some(format!("disconnected ({state})")),
        () = stats => None,
    };
    if let Some(reason) = reason {
        reporter.report(Event::FriendLeft { label, reason });
    }
    peer.close().await;
    // Closing the connection ends every bridge; give them a moment to be counted as gone.
    let deadline = Instant::now() + SHUTDOWN_TIMEOUT;
    while connections.get() > 0 && Instant::now() < deadline {
        sleep(Duration::from_millis(10)).await;
    }
    drop(slot);
}

/// What came of the attempt to bring in one friend.
enum Attempt {
    Connected(Session, Option<String>),
    Cancelled,
    StdinClosed,
}

pub async fn run(
    options: HostOptions,
    mut console: Console,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let reporter = options.reporter.clone();
    reporter.report(Event::Hosting {
        server: options.server.clone(),
    });
    let peers = Counter::default();
    let mut sessions = JoinSet::new();
    let mut friend = 1;
    let mut stop = shutdown.clone();

    loop {
        while sessions.try_join_next().is_some() {}

        let stdin_open = tokio::select! {
            () = control::stop_requested(&mut stop) => break,
            open = next_friend(&options, &peers, &mut console, friend, &mut sessions, &shutdown) => open,
        };
        if !stdin_open {
            reporter.report(Event::StdinClosed);
            control::stop_requested(&mut stop).await;
            break;
        }
        friend += 1;
    }

    reporter.report(Event::ShuttingDown);
    let _ = timeout(SHUTDOWN_TIMEOUT, sessions.join_all()).await;
    Ok(())
}

/// One round of the invite loop. Returns false when stdin is gone.
async fn next_friend(
    options: &HostOptions,
    peers: &Counter,
    console: &mut Console,
    friend: usize,
    sessions: &mut JoinSet<()>,
    shutdown: &watch::Receiver<bool>,
) -> bool {
    let reporter = &options.reporter;
    if let Some(limit) = options.max_peers {
        if peers.get() >= limit {
            reporter.report(Event::WaitingForSlot { limit });
            peers.wait_below(limit).await;
        }
    }

    let default_label = format!("friend-{friend}");
    reporter.report(Event::AskName {
        friend,
        default: default_label.clone(),
    });
    let Some(name) = console.read_line().await else {
        return false;
    };
    let label = match name.trim() {
        "" => default_label,
        name => name.to_owned(),
    };

    match invite(options, peers, console, label).await {
        Ok(Attempt::Connected(session, route)) => {
            let label = session.tunnel.label.clone();
            reporter.report(Event::FriendConnected { label, route });
            sessions.spawn(supervise(session, shutdown.clone(), reporter.clone()));
        }
        Ok(Attempt::Cancelled) => reporter.report(Event::Cancelled),
        Ok(Attempt::StdinClosed) => return false,
        Err(error) => reporter.report(Event::ConnectFailed {
            error: format!("{error:#}"),
        }),
    }

    reporter.report(Event::AskNext);
    console.read_line().await.is_some()
}

async fn invite(
    options: &HostOptions,
    peers: &Counter,
    console: &mut Console,
    label: String,
) -> Result<Attempt> {
    let reporter = &options.reporter;
    let peer = Peer::new(options.peer.clone(), refuse_channels()).await?;
    let result = async {
        let control = peer.create_channel(control::LABEL).await?;

        reporter.report(Event::Gathering);
        let sdp = peer.create_offer().await?;
        let payload = Payload::new(PayloadType::Offer, sdp, DEFAULT_TTL, options.edition);
        let invite = if options.raw_json {
            payload.encode_raw_json()?
        } else {
            payload.encode()?
        };
        reporter.report(Event::Invite {
            label: label.clone(),
            text: invite,
            minutes: DEFAULT_TTL.as_secs() / 60,
        });

        let answer = match read_reply(options, console, &label).await {
            Reply::Sdp(sdp) => sdp,
            Reply::Cancelled => return Ok(None),
            Reply::StdinClosed => return Ok(Some(None)),
        };
        reporter.report(Event::Connecting {
            label: label.clone(),
        });
        debug!(peer = label, "starting connectivity checks from the friend's reply");
        peer.accept_answer(answer).await?;
        debug!(peer = label, timeout = ?CONNECT_TIMEOUT, "ICE checks started; waiting for the peer connection");
        peer.wait_connected(CONNECT_TIMEOUT).await?;
        let route = peer.log_selected_pair(&label).await;
        Ok::<_, anyhow::Error>(Some(Some((control, route))))
    }
    .await;

    match result {
        Ok(Some(Some((control, route)))) => {
            let tunnel = Tunnel {
                peer,
                control,
                edition: options.edition,
                server: options.server.clone(),
                max_connections: options.max_connections,
                label,
                connections: Counter::default(),
            };
            let slot = peers.acquire();
            Ok(Attempt::Connected(Session { tunnel, slot }, route))
        }
        Ok(Some(None)) => {
            peer.close().await;
            Ok(Attempt::StdinClosed)
        }
        Ok(None) => {
            peer.close().await;
            Ok(Attempt::Cancelled)
        }
        Err(error) => {
            peer.close().await;
            Err(error)
        }
    }
}

enum Reply {
    Sdp(String),
    Cancelled,
    StdinClosed,
}

/// Asks until a valid reply arrives, so a bad paste does not cost the whole invite.
async fn read_reply(options: &HostOptions, console: &mut Console, label: &str) -> Reply {
    let reporter = &options.reporter;
    let deadline = Instant::now() + DEFAULT_TTL;
    loop {
        reporter.report(Event::AskReply {
            label: label.to_owned(),
        });
        let Ok(paste) = tokio::time::timeout_at(deadline, console.read_paste()).await else {
            reporter.report(Event::InviteExpired);
            return Reply::Cancelled;
        };
        let Some(paste) = paste else {
            return Reply::StdinClosed;
        };
        if paste.trim().eq_ignore_ascii_case("cancel") {
            return Reply::Cancelled;
        }
        let checked = Payload::parse(&paste, PayloadType::Answer).and_then(|payload| {
            payload.check_edition(options.edition)?;
            if let Some(whitelist) = &options.whitelist {
                check_whitelist(whitelist, &payload.sdp)?;
            }
            Ok(payload.sdp)
        });
        match checked {
            Ok(sdp) => {
                info!(peer = label, "reply accepted");
                return Reply::Sdp(sdp);
            }
            Err(error) => reporter.report(Event::BadPaste {
                error: format!("{error:#}"),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn whitelist_needs_every_fingerprint_to_match() {
        let whitelist: HashSet<String> = ["sha-256 aa:bb".to_owned()].into();
        check_whitelist(&whitelist, "a=fingerprint:sha-256 AA:BB\r\n").unwrap();

        let unknown = check_whitelist(&whitelist, "a=fingerprint:sha-256 cc:dd\r\n").unwrap_err();
        assert!(unknown.to_string().contains("sha-256 cc:dd"), "{unknown}");

        let mixed = "a=fingerprint:sha-256 aa:bb\r\na=fingerprint:sha-256 cc:dd\r\n";
        assert!(check_whitelist(&whitelist, mixed).is_err());
        assert!(check_whitelist(&whitelist, "v=0\r\n").is_err());
    }
}
