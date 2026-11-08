//! Join mode: the local listener, one data channel per connection.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use bytes::BytesMut;
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{mpsc, watch};
use tokio::time::{sleep, timeout, Instant};
use tracing::{debug, warn};
use webrtc::data_channel::DataChannel;

use crate::bridge::{bridge, Counter, Slot};
use crate::console::Console;
use crate::control::{self, channel_label, stream_id, ControlEnd, PendingOpens, Role, Transport};
use crate::datagram::{relay, Endpoint, MAX_DATAGRAM};
use crate::edition::Edition;
use crate::events::{Event, Reporter};
use crate::host::report_stats;
use crate::invite::{Payload, PayloadType, DEFAULT_TTL};
use crate::peer::{certificate_id, generate_certificate, ChannelHandler, Peer, PeerOptions};

/// The host needs time to receive and paste the reply, so this is as long as a reply stays valid.
const CONNECT_TIMEOUT: Duration = DEFAULT_TTL;
const CONTROL_TIMEOUT: Duration = Duration::from_secs(30);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Datagrams held for a flow whose channel is still opening. RakNet resends what is dropped.
const FLOW_QUEUE: usize = 64;

/// Unlike TCP, a UDP flow needs no handshake to exist, so anything able to reach the socket could
/// otherwise open channels without end by sending from new ports.
const MAX_FLOWS: usize = 64;

const UDP_ERROR_PAUSE: Duration = Duration::from_millis(10);

pub struct JoinOptions {
    pub peer: PeerOptions,
    pub edition: Edition,
    pub listen: String,
    pub raw_json: bool,
    pub reporter: Reporter,
}

/// Where Minecraft connects. Bedrock takes the same port on both protocols, because a server is
/// reached over TCP when it signals with NetherNet and over UDP when it speaks RakNet. NetherNet
/// only needs the signaling carried: the game's own WebRTC connects to the server by itself.
struct Listener {
    tcp: TcpListener,
    udp: Option<UdpSocket>,
}

impl Listener {
    async fn bind(edition: Edition, address: &str) -> io::Result<Self> {
        let tcp = TcpListener::bind(address).await?;
        let udp = match edition {
            Edition::Java => None,
            Edition::Bedrock => Some(UdpSocket::bind(tcp.local_addr()?).await?),
        };
        Ok(Self { tcp, udp })
    }
}

/// The join side of an established session.
#[derive(Clone)]
pub struct Tunnel {
    pub peer: Peer,
    pub control: Arc<dyn DataChannel>,
    pub pending: PendingOpens,
    pub connections: Counter,
    /// Shared by the listeners, because every connection needs its own stream ID.
    pub next_id: Arc<AtomicU64>,
}

impl Tunnel {
    /// Accepts TCP connections until `shutdown`, bridging each over a new channel. The bridges
    /// live on until their connection or the peer connection ends.
    pub async fn serve(self, listener: TcpListener, mut shutdown: watch::Receiver<bool>) {
        loop {
            let accepted = tokio::select! {
                () = control::stop_requested(&mut shutdown) => return,
                accepted = listener.accept() => accepted,
            };
            match accepted {
                Ok((stream, address)) => {
                    debug!(%address, "accepted a connection");
                    let id = self.next_id.fetch_add(1, Ordering::Relaxed);
                    tokio::spawn(self.clone().connect(stream, id));
                }
                Err(error) => {
                    warn!(%error, "could not accept a connection");
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    /// Reads datagrams until `shutdown`, starting a flow for each client address it has not seen.
    /// Flows live on until they go quiet or the peer connection ends.
    pub async fn serve_udp(self, socket: UdpSocket, mut shutdown: watch::Receiver<bool>) {
        let socket = Arc::new(socket);
        let port = socket.local_addr().map_or(0, |address| address.port());
        let mut flows = HashMap::<SocketAddr, mpsc::Sender<BytesMut>>::new();
        let mut buffer = vec![0; MAX_DATAGRAM];
        loop {
            let received = tokio::select! {
                () = control::stop_requested(&mut shutdown) => return,
                received = socket.recv_from(&mut buffer) => received,
            };
            let (read, client) = match received {
                Ok(received) => received,
                // Windows reports a client that went away as an error on this shared socket.
                Err(error) => {
                    debug!(%error, "could not read a datagram");
                    sleep(UDP_ERROR_PAUSE).await;
                    continue;
                }
            };
            let data = BytesMut::from(&buffer[..read]);

            if let Some(flow) = flows.get(&client).filter(|flow| !flow.is_closed()) {
                let _ = flow.try_send(data);
                continue;
            }
            flows.retain(|_, flow| !flow.is_closed());
            let Some(slot) = self.connections.try_acquire(Some(MAX_FLOWS)) else {
                warn!(%client, "too many clients, ignoring a datagram");
                continue;
            };
            debug!(%client, "new flow");
            let (flow, inbound) = mpsc::channel(FLOW_QUEUE);
            let _ = flow.try_send(data);
            flows.insert(client, flow);
            let end = Endpoint::client(socket.clone(), client, inbound, port);
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            tokio::spawn(self.clone().connect_flow(end, slot, id));
        }
    }

    async fn connect_flow(self, end: Endpoint, _slot: Slot, id: u64) {
        let label = channel_label(id);
        let channel = match self
            .peer
            .create_datagram_channel(&label, stream_id(id))
            .await
        {
            Ok(channel) => channel,
            Err(error) => {
                warn!("{error:#}");
                return;
            }
        };
        let transport = Transport::Datagrams;
        if self
            .pending
            .request(self.control.as_ref(), id, transport)
            .await
        {
            relay(end, channel).await;
        } else {
            warn!(channel = %label, "the host refused a connection");
            let _ = channel.close().await;
        }
        debug!(channel = %label, "flow closed");
    }

    async fn connect(self, stream: TcpStream, id: u64) {
        let _slot = self.connections.acquire();
        let label = channel_label(id);
        let channel = match self
            .peer
            .create_negotiated_channel(&label, stream_id(id))
            .await
        {
            Ok(channel) => channel,
            Err(error) => {
                warn!("{error:#}");
                return;
            }
        };
        let transport = Transport::Stream;
        if self
            .pending
            .request(self.control.as_ref(), id, transport)
            .await
        {
            bridge(stream, channel).await;
        } else {
            warn!(channel = %label, "the host refused a connection");
            let _ = channel.close().await;
        }
        debug!(channel = %label, "connection closed");
    }
}

/// Hands the host's `control` channel to `control` and refuses anything else.
fn control_handler(control: mpsc::Sender<Arc<dyn DataChannel>>) -> ChannelHandler {
    Arc::new(move |channel| {
        let control = control.clone();
        tokio::spawn(async move {
            if channel
                .label()
                .await
                .is_ok_and(|label| label == control::LABEL)
            {
                let _ = control.try_send(channel);
            } else {
                warn!("refusing an unknown data channel from the host");
                let _ = channel.close().await;
            }
        });
    })
}

pub async fn run(
    mut options: JoinOptions,
    mut console: Console,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let reporter = options.reporter.clone();
    // Bind first: finding out the port is taken is better before the invite dance than after it.
    let listener = Listener::bind(options.edition, &options.listen)
        .await
        .with_context(|| {
            format!(
                "could not listen on {0}. Is something already using it? If the Minecraft server \
                 runs on this machine, pick another port with --listen {1}",
                options.listen,
                options.edition.spare_address()
            )
        })?;

    let certificate = match options.peer.certificate.take() {
        Some(certificate) => certificate,
        None => generate_certificate()?,
    };
    reporter.report(Event::YourId {
        id: certificate_id(&certificate)?,
    });
    options.peer.certificate = Some(certificate);

    let edition = options.edition;
    let Some(sdp) = read_invite(edition, &mut console, &mut shutdown, &reporter).await else {
        return Ok(());
    };

    let (control_tx, mut control_rx) = mpsc::channel(1);
    let peer = Peer::new(options.peer, control_handler(control_tx)).await?;
    let session = async {
        reporter.report(Event::Gathering);
        let sdp = peer.accept_offer(sdp).await?;
        let payload = Payload::new(PayloadType::Answer, sdp, DEFAULT_TTL, edition);
        let reply = if options.raw_json {
            payload.encode_raw_json()?
        } else {
            payload.encode()?
        };
        reporter.report(Event::Reply { text: reply });
        reporter.report(Event::WaitingForHost);

        debug!(timeout = ?CONNECT_TIMEOUT, "answer sent; waiting for the host's ICE checks and peer connection");
        peer.wait_connected(CONNECT_TIMEOUT).await?;
        let route = peer.log_selected_pair("host").await;
        let control = timeout(CONTROL_TIMEOUT, control_rx.recv())
            .await
            .ok()
            .flatten()
            .ok_or_else(|| anyhow!("the host never opened its control channel"))?;
        Ok::<_, anyhow::Error>((control, route))
    };

    let (control, route) = tokio::select! {
        () = control::stop_requested(&mut shutdown) => {
            peer.close().await;
            return Ok(());
        }
        session = session => match session {
            Ok(session) => session,
            Err(error) => {
                peer.close().await;
                return Err(error);
            }
        },
    };

    let address = listener.tcp.local_addr()?.to_string();
    reporter.report(Event::Joined { address, route });
    let connections = Counter::default();
    let pending = PendingOpens::default();
    let tunnel = Tunnel {
        peer: peer.clone(),
        control: control.clone(),
        pending: pending.clone(),
        connections: connections.clone(),
        next_id: Arc::default(),
    };
    let (stop_serving, serving) = watch::channel(false);
    let datagrams = listener
        .udp
        .map(|socket| tokio::spawn(tunnel.clone().serve_udp(socket, serving.clone())));
    let server = tokio::spawn(tunnel.serve(listener.tcp, serving));

    let (rtt, measured) = watch::channel(None);
    let stats = report_stats(
        "host".to_owned(),
        connections.clone(),
        measured,
        reporter.clone(),
    );
    let end = tokio::select! {
        end = control::run(control, Role::Joiner(pending), shutdown, rtt) => end,
        state = peer.wait_lost() => {
            debug!(%state, "connection lost");
            ControlEnd::Closed
        }
        () = stats => ControlEnd::Closed,
    };

    stop_serving.send_replace(true);
    let _ = server.await;
    if let Some(datagrams) = datagrams {
        let _ = datagrams.await;
    }
    peer.close().await;
    let deadline = Instant::now() + SHUTDOWN_TIMEOUT;
    while connections.get() > 0 && Instant::now() < deadline {
        sleep(Duration::from_millis(10)).await;
    }

    match end {
        ControlEnd::PeerLeft => reporter.report(Event::Left {
            reason: "The host closed the connection.".to_owned(),
        }),
        ControlEnd::Shutdown => reporter.report(Event::Left {
            reason: "Disconnected.".to_owned(),
        }),
        ControlEnd::Closed => bail!("the connection to the host was lost"),
        ControlEnd::TimedOut => bail!("the host stopped responding"),
    }
    Ok(())
}

/// Asks until a valid invite arrives. `None` means stop.
async fn read_invite(
    edition: Edition,
    console: &mut Console,
    shutdown: &mut watch::Receiver<bool>,
    reporter: &Reporter,
) -> Option<String> {
    loop {
        reporter.report(Event::AskInvite);
        let paste = tokio::select! {
            () = control::stop_requested(shutdown) => return None,
            paste = console.read_paste() => paste?,
        };
        let checked = Payload::parse(&paste, PayloadType::Offer)
            .and_then(|payload| payload.check_edition(edition).map(|()| payload.sdp));
        match checked {
            Ok(sdp) => return Some(sdp),
            Err(error) => reporter.report(Event::BadPaste {
                error: format!("{error:#}"),
            }),
        }
    }
}
