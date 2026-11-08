//! `PeerConnection` setup, non-trickle offer/answer helpers and connection monitoring.

use std::collections::HashSet;
use std::fmt;
use std::fs;
use std::net::UdpSocket;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use rtc::peer_connection::certificate::CertificateParams;
use tokio::sync::watch;
use tokio::time::timeout;
use tracing::{debug, info, warn};
use webrtc::data_channel::{DataChannel, RTCDataChannelInit};
use webrtc::peer_connection::{
    crypto, PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCCertificate,
    RTCConfigurationBuilder, RTCIceCandidateType, RTCIceConnectionState, RTCIceGatheringState,
    RTCIceServer, RTCPeerConnectionState, RTCSessionDescription, SettingEngineBuilder,
};

use crate::config::IceConfig;
use crate::invite::DEFAULT_TTL;

/// How long to wait for candidates. A dead STUN server would otherwise hold up the invite for
/// its full retransmission schedule, and the candidates found so far are usually enough.
const GATHER_TIMEOUT: Duration = Duration::from_secs(10);

/// Signaling is a human carrying a string between two chat windows. The joiner starts ICE checks
/// when it makes the reply, possibly minutes before the host pastes it. Those checks must outlive
/// that wait, because they are what opens the joiner's NAT. The library defaults stop probing
/// after 1.4 seconds and fail after 30, so keep a slow, steady probe going for as long as an
/// invite stays valid. Dead connections are caught sooner by the control channel heartbeat.
const ICE_WAIT: Duration = Duration::from_secs(DEFAULT_TTL.as_secs() + 60);
const ICE_CHECK_INTERVAL: Duration = Duration::from_millis(500);
const ICE_MAX_CHECKS: u16 = (ICE_WAIT.as_millis() / ICE_CHECK_INTERVAL.as_millis()) as u16;

pub const FAILURE_HINT: &str =
    "Connection failed. Probably strict NAT. Configure a TURN server with --turn.";

/// Called for each data channel the remote side opens. Runs on the connection's driver task, so
/// it must return immediately; spawn anything that waits.
pub type ChannelHandler = Arc<dyn Fn(Arc<dyn DataChannel>) + Send + Sync>;

#[derive(Clone)]
pub struct PeerOptions {
    pub ice: IceConfig,
    /// Local UDP sockets. A wildcard means one socket per non-loopback interface.
    pub bind: Vec<String>,
    /// A stable DTLS identity. `None` generates a throwaway certificate.
    pub certificate: Option<RTCCertificate>,
}

impl PeerOptions {
    pub fn new(ice: IceConfig) -> Self {
        let mut bind = vec!["0.0.0.0:0".to_owned()];
        if has_ipv6_route() {
            bind.push("[::]:0".to_owned());
        }
        Self {
            ice,
            bind,
            certificate: None,
        }
    }

    /// Host candidates on loopback only, for tests that must not touch the network.
    #[cfg(test)]
    pub fn loopback() -> Self {
        Self {
            ice: IceConfig::default(),
            bind: vec!["127.0.0.1:0".to_owned()],
            certificate: None,
        }
    }
}

/// Without a routable IPv6 address, a `[::]` bind only yields a wildcard socket that can reach
/// nobody and fails every STUN request. Connecting a UDP socket only consults the routing table,
/// so this sends no packets.
fn has_ipv6_route() -> bool {
    UdpSocket::bind("[::]:0")
        .and_then(|socket| socket.connect("[2001:4860:4860::8888]:53"))
        .is_ok()
}

struct Handler {
    gathered: watch::Sender<bool>,
    state: watch::Sender<RTCPeerConnectionState>,
    on_channel: ChannelHandler,
}

#[async_trait]
impl PeerConnectionEventHandler for Handler {
    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        debug!(%state, "ICE gathering state");
        if state == RTCIceGatheringState::Complete {
            self.gathered.send_replace(true);
        }
    }

    async fn on_ice_connection_state_change(&self, state: RTCIceConnectionState) {
        debug!(%state, "ICE connection state");
    }

    async fn on_connection_state_change(&self, state: RTCPeerConnectionState) {
        debug!(%state, "peer connection state");
        self.state.send_replace(state);
    }

    async fn on_data_channel(&self, channel: Arc<dyn DataChannel>) {
        (self.on_channel)(channel);
    }
}

/// One remote friend. Cloning shares the same connection.
#[derive(Clone)]
pub struct Peer {
    pc: Arc<dyn PeerConnection>,
    gathered: watch::Receiver<bool>,
    state: watch::Receiver<RTCPeerConnectionState>,
    state_tx: watch::Sender<RTCPeerConnectionState>,
}

impl Peer {
    pub async fn new(options: PeerOptions, on_channel: ChannelHandler) -> Result<Self> {
        let (gathered_tx, gathered) = watch::channel(false);
        let (state_tx, state) = watch::channel(RTCPeerConnectionState::New);

        let mut configuration =
            RTCConfigurationBuilder::new().with_ice_servers(ice_servers(&options.ice));
        if let Some(certificate) = options.certificate {
            configuration = configuration.with_certificates(vec![certificate]);
        }
        let settings = SettingEngineBuilder::default()
            .with_ice_connection_attempts(Some(ICE_CHECK_INTERVAL), Some(ICE_MAX_CHECKS))
            .with_ice_timeouts(None, Some(ICE_WAIT), None)
            .build();
        let pc = PeerConnectionBuilder::new()
            .with_configuration(configuration.build())
            .with_setting_engine(settings)
            .with_handler(Arc::new(Handler {
                gathered: gathered_tx,
                state: state_tx.clone(),
                on_channel,
            }))
            .with_udp_addrs(options.bind)
            .build()
            .await
            .context("could not create the peer connection")?;

        Ok(Self {
            pc: Arc::new(pc),
            gathered,
            state,
            state_tx,
        })
    }

    pub async fn create_channel(&self, label: &str) -> Result<Arc<dyn DataChannel>> {
        self.pc
            .create_data_channel(label, Some(RTCDataChannelInit::default()))
            .await
            .with_context(|| format!("could not create data channel {label}"))
    }

    /// A channel both sides create themselves with the same stream ID.
    pub async fn create_negotiated_channel(
        &self,
        label: &str,
        stream_id: u16,
    ) -> Result<Arc<dyn DataChannel>> {
        let options = RTCDataChannelInit {
            negotiated: Some(stream_id),
            ..Default::default()
        };
        self.pc
            .create_data_channel(label, Some(options))
            .await
            .with_context(|| format!("could not create data channel {label}"))
    }

    /// Like [`Peer::create_negotiated_channel`], but for a flow of datagrams: nothing is resent
    /// and nothing waits for what came before.
    pub async fn create_datagram_channel(
        &self,
        label: &str,
        stream_id: u16,
    ) -> Result<Arc<dyn DataChannel>> {
        let options = RTCDataChannelInit {
            ordered: false,
            max_retransmits: Some(0),
            negotiated: Some(stream_id),
            ..Default::default()
        };
        self.pc
            .create_data_channel(label, Some(options))
            .await
            .with_context(|| format!("could not create data channel {label}"))
    }

    /// Creates the offer and returns its SDP once gathering finished, candidates included.
    pub async fn create_offer(&self) -> Result<String> {
        let offer = self
            .pc
            .create_offer(None)
            .await
            .context("could not create the offer")?;
        self.pc
            .set_local_description(offer)
            .await
            .context("could not apply the offer")?;
        self.gathered_sdp().await
    }

    /// Applies a remote offer and returns the answer SDP once gathering finished.
    pub async fn accept_offer(&self, sdp: String) -> Result<String> {
        let offer = RTCSessionDescription::offer(sdp).context("the invite has an invalid SDP")?;
        let candidates = CandidateCounts::from_sdp(&offer.sdp);
        debug!(
            host = candidates.host,
            srflx = candidates.srflx,
            relay = candidates.relay,
            other = candidates.other,
            "applying remote offer"
        );
        self.pc
            .set_remote_description(offer)
            .await
            .context("could not apply the invite")?;
        let answer = self
            .pc
            .create_answer(None)
            .await
            .context("could not create the reply")?;
        self.pc
            .set_local_description(answer)
            .await
            .context("could not apply the reply")?;
        self.gathered_sdp().await
    }

    pub async fn accept_answer(&self, sdp: String) -> Result<()> {
        let answer = RTCSessionDescription::answer(sdp).context("the reply has an invalid SDP")?;
        let candidates = CandidateCounts::from_sdp(&answer.sdp);
        debug!(
            host = candidates.host,
            srflx = candidates.srflx,
            relay = candidates.relay,
            other = candidates.other,
            "applying remote answer and starting ICE checks"
        );
        self.pc
            .set_remote_description(answer)
            .await
            .context("could not apply the reply")?;
        debug!("remote answer applied; ICE connectivity checks are running");
        Ok(())
    }

    async fn gathered_sdp(&self) -> Result<String> {
        let started = tokio::time::Instant::now();
        let mut gathered = self.gathered.clone();
        if timeout(GATHER_TIMEOUT, gathered.wait_for(|done| *done))
            .await
            .is_err()
        {
            warn!("ICE gathering is taking too long, continuing with the candidates found so far");
        }
        let description = self
            .pc
            .local_description()
            .await
            .ok_or_else(|| anyhow!("no local description after gathering"))?;
        let sdp = trim_candidates(&description.sdp);
        let candidates = CandidateCounts::from_sdp(&sdp);
        debug!(
            elapsed = ?started.elapsed(),
            host = candidates.host,
            srflx = candidates.srflx,
            relay = candidates.relay,
            other = candidates.other,
            "local ICE candidate gathering finished"
        );
        Ok(sdp)
    }

    /// Waits for the connection to come up, reporting failure with a NAT hint.
    pub async fn wait_connected(&self, limit: Duration) -> Result<()> {
        let started = tokio::time::Instant::now();
        debug!(?limit, "waiting for ICE, DTLS, and data-channel connection");
        let mut state = self.state.clone();
        let reached = timeout(
            limit,
            state.wait_for(|state| {
                matches!(
                    state,
                    RTCPeerConnectionState::Connected
                        | RTCPeerConnectionState::Failed
                        | RTCPeerConnectionState::Closed
                )
            }),
        )
        .await;
        match reached {
            Ok(Ok(state)) if *state == RTCPeerConnectionState::Connected => {
                debug!(elapsed = ?started.elapsed(), "peer connection established");
                Ok(())
            }
            Ok(Ok(state)) if *state == RTCPeerConnectionState::Closed => {
                warn!(elapsed = ?started.elapsed(), "peer connection closed before it was established");
                bail!("the connection was closed before it was established")
            }
            Ok(Ok(state)) => {
                let state = *state;
                warn!(elapsed = ?started.elapsed(), connection_state = %state, "peer connection failed during ICE/DTLS setup");
                bail!("{FAILURE_HINT} Connection state: {state}.")
            }
            Ok(Err(_)) => {
                warn!(elapsed = ?started.elapsed(), "peer connection state monitor stopped before connection");
                bail!("{FAILURE_HINT}")
            }
            Err(_) => {
                let state = *self.state.borrow();
                warn!(
                    elapsed = ?started.elapsed(),
                    connection_state = %state,
                    "timed out waiting for peer connection"
                );
                bail!("{FAILURE_HINT} Timed out in connection state {state}.")
            }
        }
    }

    /// Resolves once the connection is gone for good.
    pub async fn wait_lost(&self) -> RTCPeerConnectionState {
        let mut state = self.state.clone();
        let result = state
            .wait_for(|state| {
                matches!(
                    state,
                    RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed
                )
            })
            .await
            .map(|state| *state);
        result.unwrap_or(RTCPeerConnectionState::Closed)
    }

    /// The nominated candidate pair, available once connected.
    pub async fn selected_pair(&self) -> Option<SelectedPair> {
        let sctp = self.pc.sctp().await?;
        let ice = sctp.transport().ice_transport();
        let pair = ice.get_selected_candidate_pair().await.ok()??;
        Some(SelectedPair {
            local: pair.local().typ,
            remote: pair.remote().typ,
        })
    }

    /// Logs how this connection is routed, and returns it for display.
    pub async fn log_selected_pair(&self, label: &str) -> Option<String> {
        let pair = self.selected_pair().await;
        match &pair {
            Some(pair) => info!(peer = label, "connected via {pair}"),
            None => debug!(peer = label, "no selected candidate pair yet"),
        }
        pair.map(|pair| pair.to_string())
    }

    pub async fn close(&self) {
        if let Err(error) = self.pc.close().await {
            debug!(%error, "error while closing the peer connection");
        }
        // A local close stops the driver before it can report this itself.
        self.state_tx.send_replace(RTCPeerConnectionState::Closed);
    }
}

/// Shortens an SDP for pasting by dropping candidates that add nothing: the RTCP component, which
/// a data channel never uses, repeats of an address, and wildcards nobody can reach. Terminals can
/// cut long lines (macOS at 1024 characters), and this only changes what the other side is told,
/// not the local ICE state.
fn trim_candidates(sdp: &str) -> String {
    let mut seen = HashSet::new();
    let mut trimmed = String::with_capacity(sdp.len());
    for line in sdp.split_inclusive('\n') {
        if line.starts_with("a=candidate:") {
            let fields: Vec<_> = line.split_whitespace().collect();
            // a=candidate:<foundation> <component> <protocol> <priority> <address> <port> typ <type>
            if let [_, component, protocol, _, address, port, _, kind, ..] = fields[..] {
                let wildcard = matches!(address, "::" | "0.0.0.0");
                if component != "1" || wildcard || !seen.insert((protocol, address, port, kind)) {
                    continue;
                }
            }
        }
        trimmed.push_str(line);
    }
    trimmed
}

#[derive(Default)]
struct CandidateCounts {
    host: usize,
    srflx: usize,
    relay: usize,
    other: usize,
}

impl CandidateCounts {
    /// Counts candidate types without logging the IP addresses carried in SDP.
    fn from_sdp(sdp: &str) -> Self {
        let mut counts = Self::default();
        for line in sdp.lines().filter(|line| line.starts_with("a=candidate:")) {
            let kind = line.split_whitespace().nth(7).unwrap_or_default();
            match kind {
                "host" => counts.host += 1,
                "srflx" => counts.srflx += 1,
                "relay" => counts.relay += 1,
                _ => counts.other += 1,
            }
        }
        counts
    }
}

fn ice_servers(ice: &IceConfig) -> Vec<RTCIceServer> {
    let mut servers = Vec::new();
    if !ice.stun.is_empty() {
        servers.push(RTCIceServer {
            urls: ice.stun.clone(),
            ..Default::default()
        });
    }
    for turn in &ice.turn {
        servers.push(RTCIceServer {
            urls: vec![turn.url.clone()],
            username: turn.username.clone(),
            credential: turn.password.clone(),
        });
    }
    servers
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedPair {
    pub local: RTCIceCandidateType,
    pub remote: RTCIceCandidateType,
}

impl SelectedPair {
    pub fn relayed(&self) -> bool {
        self.local == RTCIceCandidateType::Relay || self.remote == RTCIceCandidateType::Relay
    }
}

impl fmt::Display for SelectedPair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let path = if self.relayed() {
            "relayed through TURN"
        } else {
            "direct"
        };
        write!(f, "{} <-> {} ({path})", self.local, self.remote)
    }
}

/// Loads the DTLS identity at `path`, creating it on first use, so the same ID survives restarts.
pub fn load_or_create_identity(path: &Path) -> Result<RTCCertificate> {
    let provider = crypto::default_provider().map_err(|error| anyhow!("{error}"))?;
    if path.exists() {
        let pem = fs::read_to_string(path)
            .with_context(|| format!("could not read identity file {}", path.display()))?;
        return RTCCertificate::from_pem(&pem, provider.crypto())
            .map_err(|error| anyhow!("invalid identity file {}: {error}", path.display()));
    }

    let certificate = generate_certificate()?;
    let pem = certificate
        .serialize_pem()
        .map_err(|error| anyhow!("{error}"))?;
    write_private(path, pem.as_bytes())
        .with_context(|| format!("could not write identity file {}", path.display()))?;
    info!("created a new identity in {}", path.display());
    Ok(certificate)
}

/// A self-signed certificate, the same kind the library makes for itself.
pub fn generate_certificate() -> Result<RTCCertificate> {
    let provider = crypto::default_provider().map_err(|error| anyhow!("{error}"))?;
    let params =
        CertificateParams::new(vec!["mcrtc".to_owned()]).map_err(|error| anyhow!("{error}"))?;
    RTCCertificate::generate(
        provider.crypto(),
        crypto::SignatureScheme::EcdsaP256Sha256,
        params,
    )
    .map_err(|error| anyhow!("could not generate a certificate: {error}"))
}

/// The ID a friend gives the host for its whitelist.
pub fn certificate_id(certificate: &RTCCertificate) -> Result<String> {
    let provider = crypto::default_provider().map_err(|error| anyhow!("{error}"))?;
    let fingerprints = certificate
        .get_fingerprints(provider.crypto())
        .map_err(|error| anyhow!("{error}"))?;
    let fingerprint = fingerprints
        .first()
        .ok_or_else(|| anyhow!("the certificate has no fingerprint"))?;
    Ok(crate::invite::normalize_fingerprint(&format!(
        "{} {}",
        fingerprint.algorithm, fingerprint.value
    )))
}

#[cfg(unix)]
fn write_private(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?
        .write_all(contents)
}

#[cfg(not(unix))]
fn write_private(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    fs::write(path, contents)
}

#[cfg(test)]
pub mod test_support {
    use super::*;

    pub fn ignore_channels() -> ChannelHandler {
        Arc::new(|_| {})
    }

    /// Runs offer/answer in memory, so `offerer` has to have a channel already.
    pub async fn connect_in_memory(offerer: &Peer, answerer: &Peer) -> Result<()> {
        let offer = offerer.create_offer().await?;
        let answer = answerer.accept_offer(offer).await?;
        offerer.accept_answer(answer).await?;
        offerer.wait_connected(Duration::from_secs(20)).await?;
        answerer.wait_connected(Duration::from_secs(20)).await
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;
    use crate::invite::sdp_fingerprints;
    use bytes::BytesMut;
    use tokio::sync::mpsc;
    use webrtc::data_channel::DataChannelEvent;

    async fn wait_open(channel: &Arc<dyn DataChannel>) {
        loop {
            match channel.poll().await {
                Some(DataChannelEvent::OnOpen) => return,
                Some(_) => {}
                None => panic!("channel ended before opening"),
            }
        }
    }

    #[test]
    fn trimming_drops_repeats_and_wildcards_only() {
        let sdp = "v=0\r\n\
            a=candidate:1 1 udp 9 10.0.0.1 5000 typ host\r\n\
            a=candidate:2 1 udp 8 10.0.0.1 5000 typ host\r\n\
            a=candidate:3 1 udp 7 10.0.0.1 5001 typ host\r\n\
            a=candidate:4 1 udp 6 203.0.113.9 5000 typ srflx raddr 0.0.0.0 rport 0\r\n\
            a=candidate:5 1 udp 5 203.0.113.9 5000 typ srflx raddr 0.0.0.0 rport 0\r\n\
            a=candidate:6 1 udp 4 :: 5002 typ host\r\n\
            a=candidate:7 1 udp 3 0.0.0.0 5003 typ host\r\n\
            a=candidate:8 2 udp 2 10.0.0.2 5004 typ host\r\n\
            a=mid:0\r\n";
        let trimmed = trim_candidates(sdp);
        assert_eq!(trimmed.matches("a=candidate").count(), 3, "{trimmed}");
        for kept in [
            "a=candidate:1 ",
            "a=candidate:3 ",
            "a=candidate:4 ",
            "v=0\r\n",
            "a=mid:0\r\n",
        ] {
            assert!(trimmed.contains(kept), "{kept} missing from {trimmed}");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn single_channel_echo() {
        let echoer: ChannelHandler = Arc::new(|channel| {
            tokio::spawn(async move {
                while let Some(event) = channel.poll().await {
                    if let DataChannelEvent::OnMessage(message) = event {
                        channel.send(message.data).await.unwrap();
                    }
                }
            });
        });
        let offerer = Peer::new(PeerOptions::loopback(), ignore_channels())
            .await
            .unwrap();
        let answerer = Peer::new(PeerOptions::loopback(), echoer).await.unwrap();
        let channel = offerer.create_channel("echo").await.unwrap();
        connect_in_memory(&offerer, &answerer).await.unwrap();

        wait_open(&channel).await;
        channel.send(BytesMut::from(&b"hello"[..])).await.unwrap();
        let reply = timeout(Duration::from_secs(10), async {
            loop {
                match channel.poll().await {
                    Some(DataChannelEvent::OnMessage(message)) => return message.data,
                    Some(_) => {}
                    None => panic!("channel ended before the echo"),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(&reply[..], b"hello");

        let pair = offerer.selected_pair().await.expect("a selected pair");
        assert_eq!(pair.local, RTCIceCandidateType::Host);
        assert!(!pair.relayed());
        assert!(pair.to_string().contains("direct"));

        offerer.close().await;
        answerer.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn losing_the_remote_side_is_reported() {
        let offerer = Peer::new(PeerOptions::loopback(), ignore_channels())
            .await
            .unwrap();
        let answerer = Peer::new(PeerOptions::loopback(), ignore_channels())
            .await
            .unwrap();
        let _channel = offerer.create_channel("control").await.unwrap();
        connect_in_memory(&offerer, &answerer).await.unwrap();

        offerer.close().await;
        let state = timeout(Duration::from_secs(5), offerer.wait_lost()).await;
        assert_eq!(state.unwrap(), RTCPeerConnectionState::Closed);
        answerer.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_unreachable_peer_reports_the_hint() {
        let offerer = Peer::new(PeerOptions::loopback(), ignore_channels())
            .await
            .unwrap();
        let answerer = Peer::new(PeerOptions::loopback(), ignore_channels())
            .await
            .unwrap();
        let _channel = offerer.create_channel("control").await.unwrap();
        let offer = offerer.create_offer().await.unwrap();
        let answer = answerer.accept_offer(offer).await.unwrap();
        answerer.close().await;

        offerer.accept_answer(answer).await.unwrap();
        let error = offerer
            .wait_connected(Duration::from_secs(2))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("--turn"), "{error}");
        offerer.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn identity_survives_restarts_and_appears_in_the_sdp() {
        let directory = std::env::temp_dir().join(format!("mcrtc-test-{}", std::process::id()));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("identity.pem");
        let _ = fs::remove_file(&path);

        let first = load_or_create_identity(&path).unwrap();
        let second = load_or_create_identity(&path).unwrap();
        let id = certificate_id(&first).unwrap();
        assert_eq!(id, certificate_id(&second).unwrap());

        let mut options = PeerOptions::loopback();
        options.certificate = Some(second);
        let peer = Peer::new(options, ignore_channels()).await.unwrap();
        let _channel = peer.create_channel("control").await.unwrap();
        let sdp = peer.create_offer().await.unwrap();
        assert!(sdp_fingerprints(&sdp).contains(&id), "{id} not in {sdp}");
        peer.close().await;
        fs::remove_dir_all(&directory).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn offer_has_an_application_section_and_candidates() {
        let (tx, mut rx) = mpsc::channel(1);
        let peer = Peer::new(PeerOptions::loopback(), ignore_channels())
            .await
            .unwrap();
        let _channel = peer.create_channel("control").await.unwrap();
        tx.send(peer.create_offer().await.unwrap()).await.unwrap();
        let sdp = rx.recv().await.unwrap();
        assert!(sdp.contains("m=application"), "{sdp}");
        assert!(sdp.contains("a=candidate"), "{sdp}");
        peer.close().await;
    }
}
