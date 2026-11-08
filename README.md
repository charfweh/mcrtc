# mcrtc
asdasdasd
Let friends join your locally hosted, unmodified Minecraft server, Java or Bedrock, without port
forwarding or a VPN. mcrtc tunnels Minecraft's traffic over encrypted WebRTC data channels. There
is no signaling server: you and your friend exchange one short string each over Discord, WhatsApp
or anything else.

```
Friend's MC client -> 127.0.0.1:25565 -> [mcrtc join] ~~WebRTC~~ [mcrtc host] -> 127.0.0.1:25565 -> MC server
```

Bedrock uses port 19132 in place of 25565, and mcrtc carries both TCP and UDP on it: NetherNet
servers signal over TCP, and RakNet servers use UDP. Whether this gets a player into a current
server is not proven yet: see [Limitations](#limitations). Both ends need mcrtc, so a friend needs
a PC to run it on; consoles cannot run it.

![Title screen](docs/title-screen.png)

> **Warning:** invites and replies contain your IP addresses. Only share them with people you
> trust, and do not post them publicly.

## Quick start with the interface

Run `mcrtc` with no arguments, or double-click it. A native window opens, drawn like Minecraft's
own menus, with the same automatic GUI scale. Keep it open while you play: closing it ends the
session.

Pick **Java Edition** or **Bedrock Edition** on the title screen first. You and your friend must
pick the same one; an invite for the other edition is refused and says which to choose.

**Host**

1. Start your Minecraft server.
2. Choose **Host a Server**, then **Start Hosting**.
3. Choose **Invite a Friend**, enter a name, and send them the invite with **Copy Invite**.
4. Paste the reply they send back and choose **Connect**.

**Friend**

1. Choose **Join a Friend**, paste the invite, and choose **Join Server**.
2. Send the reply to the host with **Copy Reply**.
3. Once it says **Connected!**, add the address it shows as a server in Minecraft.

| | |
|---|---|
| ![Friends list](docs/friends.png) | ![Invite screen](docs/invite.png) |

## Quick start in a terminal

**Host**

```console
$ mcrtc host
Warning: invites and replies contain your IP addresses. Only share them with people you trust.
Hosting 127.0.0.1:25565. Press Ctrl+C to stop.

=== Friend 1 ===
Name for this friend [friend-1]:
alex
Gathering network candidates...

Send this INVITE to alex (valid for 10 minutes):

mcrtc1:vVdbT9swFP4rVp-Xzj6-xUfqQ0tBMC5D...

Paste alex's REPLY here (or type `cancel`):
mcrtc1:rVbbbtswDP0Vw8-zp4tpWQLy0K5F...
Connecting to alex...
INFO mcrtc::peer: connected via host <-> srflx (direct) peer="alex"
alex connected.
Press Enter for another friend.
```

**Friend**

```console
$ mcrtc join
Warning: invites and replies contain your IP addresses. Only share them with people you trust.
Your mcrtc ID: sha-256 8a:e4:ac:...
Paste the INVITE from the host:
mcrtc1:vVdbT9swFP4rVp-Xzj6-xUfqQ0tBMC5D...
Gathering network candidates...

Send this REPLY to the host:

mcrtc1:rVbbbtswDP0Vw8-zp4tpWQLy0K5F...

Waiting for the host to paste it...
INFO mcrtc::peer: connected via host <-> srflx (direct) peer="host"
Connected! Point Minecraft at 127.0.0.1:25565.
```

Then, in Minecraft: Multiplayer, Add Server, `127.0.0.1:25565`.

**Bedrock:** add `--edition bedrock` on both sides. The friend needs the port free on both TCP and
UDP. In Minecraft: Play, Servers, Add Server, with
address `127.0.0.1` and port `19132`.

**Testing on one machine:** the server and `mcrtc join` cannot both use the same port there, so
give the joiner another one with `mcrtc join --listen 127.0.0.1:25566`, or `127.0.0.1:19134` for
Bedrock.

## Options

```
mcrtc                                    Open the interface
mcrtc ui
mcrtc host [--server 127.0.0.1:25565] [--whitelist-file ids.txt]
           [--max-peers N] [--max-connections N]
mcrtc join [--listen 127.0.0.1:25565] [--identity friend.pem]

Common: --edition java|bedrock, --config mcrtc.toml, --stun <url> (repeatable),
        --turn <url> --turn-user <u> --turn-pass <p>, --raw-json, -v
```

`--raw-json` prints plain JSON instead of the `mcrtc1:` string. Either form is accepted when
pasting; end a JSON paste with a blank line. See [mcrtc.example.toml](mcrtc.example.toml) for the
config file. Flags win over the file.

`--server` and `--listen` default to port 25565 for Java and 19132 for Bedrock. In the interface,
`server` and `listen` from a config file belong to the edition it starts on, which is the file's
`edition` or `--edition`; the other edition uses its default.

The default STUN servers are `stun:stun.l.google.com:19302` and `stun:stun.cloudflare.com:3478`.

## Only letting in people you know

Each friend can keep a stable ID, which is the fingerprint of a certificate only they hold:

1. The friend runs `mcrtc join --identity friend.pem` and sends you the `Your mcrtc ID:` line.
2. You add it to a file, one per line, with `#` comments: `sha-256 8a:e4:ac:... # alex`
3. You run `mcrtc host --whitelist-file ids.txt`.

A reply is refused unless every fingerprint in it is listed. The DTLS handshake then proves the
friend owns that certificate, so an ID cannot be faked by copying it. Without `--identity`, a new
ID is generated on every run. In the interface, set `whitelist_file` and `identity` in a config
file and start it with `mcrtc ui --config mcrtc.toml`.

## How it works

1. The host creates a WebRTC offer, waits for ICE gathering to finish, and prints it as an invite.
2. The friend pastes it, creates an answer the same way, and prints it as a reply.
3. The host pastes the reply and the two sides connect, directly when possible.
4. Each TCP connection Minecraft makes to the friend's mcrtc gets its own reliable, ordered data
   channel, and the host opens a matching connection to the server.
5. For Bedrock, the friend's mcrtc also listens on UDP on the same port. A NetherNet server only
   has its HTTP signaling carried, over TCP as above, and the game's own WebRTC then connects to
   the server directly, outside the tunnel. For RakNet, each client address gets its own unordered
   data channel without retransmits, and the host gives it its own UDP socket to the server. RakNet
   handles loss and ordering itself, so resending here would only add lag. A flow ends after 30
   quiet seconds, and the friend's side allows 64 at once.

An invite is `{ "v": 1, "type": "offer"|"answer", "sdp": "...", "expires": <unix_ts> }`, deflated,
base64url encoded without padding, and prefixed with `mcrtc1:`. They expire after 10 minutes.
Bedrock strings add `"edition": "bedrock"`.

Traffic is encrypted with DTLS, and peers are verified by the certificate fingerprints in the
SDP. Reads are chunked to 16 KiB. A sender pauses above 1 MiB of unacknowledged data and resumes
below 256 KiB, and a slow TCP reader pushes back through bounded queues all the way to the remote
sender. A `control` channel carries a heartbeat, connection setup, and a clean goodbye.

### Design decisions

- **Tunnel channels are negotiated, not announced with DCEP.** The joiner still opens a new
  `mc-<id>` channel per connection, but it picks the stream ID and tells the host over `control`
  (`open <id>`, answered by `ready <id>` or `refused <id>`). With DCEP, webrtc-rs reuses the
  lowest free stream ID as soon as `close()` is called, while SCTP is still resetting the old
  stream, so the rapid reconnects of a server-list ping hang or get killed. Rotating through 30,000
  IDs removes that race and costs the same single round trip.
- **ICE keeps probing for the whole life of an invite.** The library defaults give up after 30
  seconds, and a person pasting a reply can take minutes. The probes are also what keeps the
  friend's NAT open for the host.
- **The heartbeat detects dead peers**, within 45 seconds, because the ICE failure timeout had to
  be raised for the reason above. A clean exit is noticed immediately.
- **A TCP write stalled for 30 seconds closes that connection.** One full channel makes the library
  stop reading the whole connection, so a stuck socket must not be allowed to freeze a friend's
  other streams.
- **`--whitelist-file` holds DTLS fingerprints**, as described above, since that is the identity
  that can actually be verified.
- **`--identity` was added** because a whitelist needs an ID that survives restarts.
- **The interface is a native window** drawn with [egui](https://github.com/emilk/egui), with no
  web page or local server, so nothing else on the machine can talk to it. Every control is
  painted by mcrtc: one point is one Minecraft GUI pixel, and the scale is the largest whole number
  that keeps 320x240 in the window, which keeps the pixel font sharp. The binary still links no GUI
  libraries, because OpenGL, X11 and Wayland are loaded when the window opens.
- **Half-close is not preserved.** TCP EOF tears the connection down once queued data is delivered.
  Minecraft never half-closes.
- **Invites leave out RTCP candidates, repeats and wildcards**, so they stay short enough to paste.
- **Bedrock status pongs are rewritten on the friend's side.** A server's pong advertises its own
  port and the game can connect to that instead of the one it pinged, which would miss the tunnel.
  The friend's mcrtc replaces it with the port it listens on.

## Limitations

- **Every player appears as 127.0.0.1 to the Minecraft server.** IP bans and per-IP limits cannot
  tell friends apart. Use the server's whitelist and UUID bans instead. `online-mode=true` works
  as usual.
- A connection stuck for up to 30 seconds can delay that friend's other connections.
- mcrtc does not reconnect. If a connection drops, make a new invite.
- **Bedrock has not been tested with the game itself, and NetherNet may not get through.** A
  1.26.62 dedicated server defaults to `transport=nethernet`. mcrtc carries its HTTP signaling,
  but gameplay is the game's own WebRTC connecting straight to the server, so that connection
  still has to cross both routers without mcrtc's help. `online-mode=false` turns the signaling
  port off, and `transport=raknet` logs that players cannot connect. A scripted RakNet handshake
  does get accepted by that server through the tunnel.
- Consoles cannot run mcrtc. Phones and tablets cannot either, so they would need it running on a
  PC on the same network, with `--listen` set to an address they can reach.

## TURN setup

STUN finds your public address, which is enough for most home networks. Strict or symmetric NATs,
including many mobile and campus networks, need a TURN relay. Traffic stays encrypted end to end,
but it uses the relay's bandwidth and adds latency. Only one side strictly needs TURN, though it is
simplest to configure it on both.

Pass it as flags, in the config file, or under **Options...** in the interface:

```sh
mcrtc host --turn 'turn:turn.example.com:3478?transport=udp' --turn-user USER --turn-pass PASS
```

- **coturn (self-hosted):** on a VPS with a public IP, install `coturn` and set `listening-port=3478`,
  `fingerprint`, `lt-cred-mech`, `user=USER:PASS`, `realm=example.com` and, on a NATed cloud VM,
  `external-ip=PUBLIC/PRIVATE`. Open 3478 TCP and UDP and the relay range (49152-65535 UDP).
- **Metered:** create an account at metered.ca. Its dashboard gives you TURN URLs and credentials
  with a free monthly allowance.
- **Cloudflare Realtime TURN:** create a TURN key in the Cloudflare dashboard, then request
  short-lived credentials from its API. They expire, so generate new ones when needed.

The log line `connected via ... (relayed through TURN)` confirms it is in use.

## Troubleshooting

| Problem | Try |
|---|---|
| `Connection failed. Probably strict NAT.` | Configure TURN on both sides, see above. |
| `could not listen on 127.0.0.1:25565` | The server or another program has the port. Use `--listen 127.0.0.1:25566`. |
| `this invite is for Bedrock Edition` | You and the host picked different editions. Pick the one it names. |
| Bedrock on Windows cannot see `127.0.0.1` | Older Store versions block loopback. Run `CheckNetIsolation LoopbackExempt -a -n="Microsoft.MinecraftUWP_8wekyb3d8bbwe"` as administrator. |
| Friend connects but Minecraft cannot reach the server | Check that the server is running and that `--server` matches its port. The host logs `could not reach the Minecraft server`. |
| `this invite expired` | Make a new invite. They last 10 minutes. |
| `the string is corrupted or was cut off` | Copy it again. Some chat apps cut long messages, so try sending it as a file, or use the interface. |
| Paste is cut off in a macOS terminal | macOS limits terminal lines to 1024 characters. Use the interface. |
| `could not open a window` | The interface needs a desktop session with OpenGL 2 or newer. Over SSH or on a server, use `mcrtc host` and `mcrtc join`, which work without one. |
| You want more detail | Add `-v`. Logs at this level include full SDPs, so they contain IP addresses. Do not post them publicly. |

## Building

```sh
cargo build --release     # target/release/mcrtc
cargo test
```

GitHub Actions ([.github/workflows/release.yml](.github/workflows/release.yml)) tests every push
and builds Windows, macOS and Linux binaries. Pushing a `v*` tag attaches them to a release.

To cross-compile yourself, use native builds or [`cross`](https://github.com/cross-rs/cross) for
Linux, and [`cargo-xwin`](https://github.com/rust-cross/cargo-xwin) for Windows, since the crypto
library builds C and assembly:

```sh
cargo install cross
cross build --release --target aarch64-unknown-linux-gnu

cargo install cargo-xwin
rustup target add x86_64-pc-windows-msvc
cargo xwin build --release --target x86_64-pc-windows-msvc

# On a Mac, for either architecture
rustup target add x86_64-apple-darwin aarch64-apple-darwin
cargo build --release --target x86_64-apple-darwin
```

## Credits

The interface uses the [Monocraft](https://github.com/IdreesInc/Monocraft) font under the SIL Open
Font License (see [assets/ui/Monocraft-OFL.txt](assets/ui/Monocraft-OFL.txt)). All textures and the
logo are original and generated by mcrtc at startup, so it ships none of Minecraft's artwork.

mcrtc is not affiliated with Mojang or Microsoft. Minecraft is a trademark of Mojang AB.
