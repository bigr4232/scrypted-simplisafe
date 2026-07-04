/**
 * LiveKit live streaming for newer SimpliSafe cameras.
 *
 * Despite reporting `providers.webrtc === 'kvs'`, newer SimpliSafe cameras actually stream over
 * LiveKit (not Amazon KVS). The `/v2/cameras/{uuid}/{location}/live-view` endpoint returns:
 *   { liveKitDetails: { liveKitURL: "wss://livestream.services.simplisafe.com:7880",
 *                       userToken: "<LiveKit access-token JWT>" }, cameraStatus: "online" }
 *
 * We connect to LiveKit as a subscriber (auto_subscribe), let the server offer its subscriber
 * peer connection, answer it with werift, and receive the camera's encoded audio/video RTP tracks.
 * Those tracks are then forwarded (no transcoding) into a second werift peer connection that is
 * negotiated with the Scrypted consumer `RTCSignalingSession`. The WeriftSignalingSession /
 * connectRTCSignalingClients helpers are adapted from @scrypted/webrtc.
 *
 * Signaling protocol reference: livekit/client-sdk-js (SignalClient/RTCEngine). Messages are
 * @livekit/protocol SignalRequest/SignalResponse protobufs sent as binary websocket frames.
 */

import WebSocket from 'ws';
import {
    MediaStreamTrack,
    RtpPacket,
    RTCIceCandidate,
    RTCPeerConnection,
    RTCRtpCodecParameters,
} from '@koush/werift';

// SimpliSafe's LiveKit cameras publish multiple video codecs (VP8/VP9/H264/AV1/H265). werift
// defaults to VP8, but Scrypted's WebRTC->RTSP path only accepts H264. Constraining the
// subscriber to H264 makes the camera send H264 directly, avoiding any transcode. These params
// mirror @scrypted/webrtc's requiredVideoCodec / Opus so the downstream negotiation lines up.
const SUBSCRIBE_VIDEO_CODECS = [
    new RTCRtpCodecParameters({
        mimeType: 'video/H264',
        clockRate: 90000,
        rtcpFeedback: [
            { type: 'transport-cc' },
            { type: 'ccm', parameter: 'fir' },
            { type: 'nack' },
            { type: 'nack', parameter: 'pli' },
            { type: 'goog-remb' },
        ],
        parameters: 'level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f',
    }),
    // Accept RTX retransmissions. Without this, the SFU has no channel to answer our NACKs and
    // most upstream packet loss is unrecoverable (observed ~0.4% net video loss -> HomeKit frame
    // drops). LiveKit's payload type mapping is stable (H264 on 125, its rtx on 126/apt=125), and
    // werift keeps every remote rtx whose apt origin matches a local codec, so mirroring one pair
    // is enough to accept them all. payloadType MUST be explicit: werift's constructor rewrites
    // the apt parameter of any rtx codec it auto-assigns a payload type to.
    new RTCRtpCodecParameters({
        mimeType: 'video/rtx',
        clockRate: 90000,
        payloadType: 126,
        parameters: 'apt=125',
    }),
];
const SUBSCRIBE_AUDIO_CODECS = [
    new RTCRtpCodecParameters({
        mimeType: 'audio/opus',
        clockRate: 48000,
        channels: 2,
        payloadType: 111,
    }),
];
// Talk-back (microphone) is published with RED (RFC 2198 redundant Opus) as the primary codec,
// matching exactly what the SimpliSafe app publishes (track name "microphone", mimeType audio/red).
// The camera only plays audio published this way. werift auto-fills the red codec's parameters to
// `${payloadType+1}/${payloadType+1}`, so opus must sit at the red payloadType + 1 (110 -> 111), and
// werift's sender then RED-wraps the forwarded Opus payloads automatically.
const PUBLISH_AUDIO_CODECS = [
    new RTCRtpCodecParameters({
        mimeType: 'audio/red',
        clockRate: 48000,
        channels: 2,
        payloadType: 110,
    }),
    new RTCRtpCodecParameters({
        mimeType: 'audio/opus',
        clockRate: 48000,
        channels: 2,
        payloadType: 111,
    }),
];
import {
    AddTrackRequest,
    MuteTrackRequest,
    SessionDescription,
    SignalRequest,
    SignalResponse,
    SignalTarget,
    TrackSource,
    TrackType,
    TrickleRequest,
} from '@livekit/protocol';
import type {
    RTCAVSignalingSetup,
    RTCSessionControl,
    RTCSignalingOptions,
    RTCSignalingSendIceCandidate,
    RTCSignalingSession,
} from '@scrypted/sdk';

export interface LiveKitDetails {
    liveKitURL: string;
    userToken: string;
}

export interface LiveViewResponse {
    liveKitDetails?: LiveKitDetails;
    cameraStatus?: string;
}

type ConsoleLike = Pick<Console, 'log' | 'warn' | 'error'>;

// Protocol/version advertised to the LiveKit server, mirroring livekit-client.
const LK_PROTOCOL = 15;
const LK_VERSION = '2.20.0';

/**
 * Flatten LiveKit ICE servers (protobuf ICEServer with a `urls` array) into werift's expected
 * shape (one entry per url string).
 */
function normalizeIceServers(iceServers: any[] | undefined): any[] {
    if (!Array.isArray(iceServers))
        return [];
    const ret: any[] = [];
    for (const ice of iceServers) {
        const urls = ice?.urls ?? ice?.url;
        const credential = ice?.credential ?? ice?.password;
        const username = ice?.username;
        if (Array.isArray(urls)) {
            for (const url of urls)
                ret.push({ urls: url, username, credential });
        }
        else if (typeof urls === 'string') {
            ret.push({ urls, username, credential });
        }
    }
    return ret;
}

const H264_START_CODE = Buffer.from([0x00, 0x00, 0x00, 0x01]);

/**
 * Reassembles the camera's H264 RTP into Annex-B access units and retains the most recent keyframe
 * (SPS + PPS + IDR). This lets the plugin produce a still from the already-flowing WebRTC video
 * without cold-starting a fresh stream — the slow, timeout-prone path that otherwise makes HomeKit
 * show "Snapshot Failed". werift's H264RtpPayload does not reassemble FU-A fragments (how keyframe
 * slices almost always arrive), so RFC 6184 depacketization is done here.
 */
class H264KeyframeCapturer {
    private sps?: Buffer;
    private pps?: Buffer;
    private auNals: Buffer[] = [];
    private auHasIdr = false;
    private auHasSps = false;
    private auHasPps = false;
    private auTimestamp?: number;
    private fuBuffer?: Buffer;
    private latest?: Buffer;

    get keyframe(): Buffer | undefined {
        return this.latest;
    }

    onRtp(rtp: RtpPacket): void {
        const buf = rtp.payload;
        if (!buf || buf.length < 1)
            return;

        // A change in RTP timestamp marks a new access unit; flush the previous one. (The marker bit
        // handled below is the primary boundary; this is a fallback when markers are unreliable.)
        const timestamp = rtp.header.timestamp;
        if (this.auTimestamp !== undefined && timestamp !== this.auTimestamp)
            this.finishAccessUnit();
        this.auTimestamp = timestamp;

        const nalType = buf[0] & 0x1f;
        if (nalType >= 1 && nalType <= 23) {
            // Single NAL unit packet.
            this.pushNal(buf);
        }
        else if (nalType === 24) {
            // STAP-A: one packet aggregating several NAL units (commonly SPS + PPS).
            let offset = 1;
            while (offset + 2 <= buf.length) {
                const size = buf.readUInt16BE(offset);
                offset += 2;
                if (size === 0 || offset + size > buf.length)
                    break;
                this.pushNal(buf.subarray(offset, offset + size));
                offset += size;
            }
        }
        else if (nalType === 28) {
            // FU-A: a single NAL unit fragmented across packets.
            if (buf.length < 2)
                return;
            const fuHeader = buf[1];
            const start = (fuHeader & 0x80) !== 0;
            const end = (fuHeader & 0x40) !== 0;
            const fragment = buf.subarray(2);
            if (start) {
                const nalHeader = (buf[0] & 0xe0) | (fuHeader & 0x1f);
                this.fuBuffer = Buffer.concat([Buffer.from([nalHeader]), fragment]);
            }
            else if (this.fuBuffer) {
                this.fuBuffer = Buffer.concat([this.fuBuffer, fragment]);
            }
            if (end && this.fuBuffer) {
                this.pushNal(this.fuBuffer);
                this.fuBuffer = undefined;
            }
        }
        // STAP-B / FU-B / MTAP are not used by these cameras and are ignored.

        if (rtp.header.marker)
            this.finishAccessUnit();
    }

    private pushNal(nal: Buffer): void {
        const type = nal[0] & 0x1f;
        if (type === 7) { this.sps = nal; this.auHasSps = true; }
        else if (type === 8) { this.pps = nal; this.auHasPps = true; }
        else if (type === 5) this.auHasIdr = true;
        this.auNals.push(nal);
    }

    private finishAccessUnit(): void {
        if (this.auHasIdr && this.auNals.length) {
            const parts: Buffer[] = [];
            // Ensure the still is decodable even if the camera sent parameter sets in an earlier AU.
            if (this.sps && !this.auHasSps) parts.push(H264_START_CODE, this.sps);
            if (this.pps && !this.auHasPps) parts.push(H264_START_CODE, this.pps);
            for (const nal of this.auNals) parts.push(H264_START_CODE, nal);
            this.latest = Buffer.concat(parts);
        }
        this.auNals = [];
        this.auHasIdr = false;
        this.auHasSps = false;
        this.auHasPps = false;
        this.fuBuffer = undefined;
        this.auTimestamp = undefined;
    }
}

/**
 * RTP sequence-continuity accounting for diagnosing packet loss. Tracks forward gaps (packets
 * skipped), late arrivals (retransmissions/reordering filling earlier gaps), and duplicates.
 * `lost` is net loss after late arrivals are credited back.
 */
class RtpSeqStats {
    received = 0;
    gapEvents = 0;
    lost = 0;
    reordered = 0;
    private expected?: number;

    onPacket(seq: number): void {
        this.received++;
        if (this.expected === undefined) {
            this.expected = (seq + 1) & 0xffff;
            return;
        }
        if (seq === this.expected) {
            this.expected = (seq + 1) & 0xffff;
            return;
        }
        const delta = (seq - this.expected) & 0xffff;
        if (delta < 0x8000) {
            // Jumped forward: delta packets were skipped.
            this.gapEvents++;
            this.lost += delta;
            this.expected = (seq + 1) & 0xffff;
        }
        else {
            // Behind the high-water mark: a late (retransmitted/reordered) packet filled a gap.
            this.reordered++;
            if (this.lost > 0)
                this.lost--;
        }
    }
}

interface LiveKitViewer {
    pc: RTCPeerConnection;
    videoTrack?: MediaStreamTrack;
    audioTrack?: MediaStreamTrack;
    close: () => void;
    /** The most recent decode-ready H264 keyframe (Annex-B SPS+PPS+IDR), if any has been seen. */
    getKeyframe: () => Buffer | undefined;
    /**
     * Lazily publish a single shared talk-back (microphone) track to LiveKit over this same
     * participant connection and return it. Callers write RTP into the returned track; it is
     * published only once and reused across all consumer sessions.
     */
    ensureMicTrack: () => Promise<MediaStreamTrack>;
    /**
     * Request exclusive use of the shared talk-back track for the calling session (identified by a
     * stable token). Returns true if this session owns the mic and may write RTP; false if another
     * session is currently talking. Unmutes on claim and re-mutes after the talker goes idle,
     * mirroring how the SimpliSafe app toggles mute per talk so the camera re-triggers playback.
     */
    claimMic: (token: object) => boolean;
    /**
     * Write a talk-back packet into the shared mic track, rewriting its sequence/timestamp to our own
     * monotonic counters so the stream stays continuous across consumer-session (source) switches.
     */
    writeMic: (rtp: RtpPacket) => void;
}

/**
 * Connect to LiveKit as a subscriber and return the negotiated peer connection plus the received
 * media tracks.
 */
async function startLiveKitViewer(details: LiveKitDetails, console: ConsoleLike, debug: () => boolean): Promise<LiveKitViewer> {
    const dlog = (...args: any[]) => { if (debug()) console.log(...args); };
    const base = details.liveKitURL.replace(/\/$/, '');
    const params = new URLSearchParams({
        access_token: details.userToken,
        auto_subscribe: '1',
        sdk: 'js',
        version: LK_VERSION,
        protocol: String(LK_PROTOCOL),
    });
    const url = `${base}/rtc?${params.toString()}`;

    dlog('SS:LiveKit connecting', base);
    const ws = new WebSocket(url);

    let pc: RTCPeerConnection | undefined;
    let videoTrack: MediaStreamTrack | undefined;
    let audioTrack: MediaStreamTrack | undefined;
    const keyframeCapturer = new H264KeyframeCapturer();
    // Packet-loss diagnostics: sequence continuity at the earliest point we see LiveKit RTP.
    const videoStats = new RtpSeqStats();
    const audioStats = new RtpSeqStats();
    let nackSends = 0;
    let statsTimer: NodeJS.Timeout | undefined;
    let lastStatsLine = '';
    const startStatsLogging = () => {
        if (statsTimer)
            return;
        statsTimer = setInterval(() => {
            const line = `SS:rtpstats video recv=${videoStats.received} lost=${videoStats.lost} gaps=${videoStats.gapEvents} late=${videoStats.reordered}`
                + ` | audio recv=${audioStats.received} lost=${audioStats.lost}`
                + ` | nackSends=${nackSends}`;
            if (line !== lastStatsLine) {
                lastStatsLine = line;
                dlog(line);
            }
        }, 10_000);
    };
    let pingTimer: NodeJS.Timeout | undefined;
    let remoteDescriptionSet = false;
    const pendingCandidates: RTCIceCandidate[] = [];

    // Publisher (outbound) peer connection state, created lazily when the first mic track is
    // published. LiveKit uses a separate publisher PC on the SAME signaling websocket/participant.
    let joinIceServers: any[] = [];
    let publisherPc: RTCPeerConnection | undefined;
    let publisherRemoteDescriptionSet = false;
    let publisherMicSender: any;
    const pendingPublisherCandidates: RTCIceCandidate[] = [];

    // Mic mute state. The SimpliSafe app toggles its published microphone track muted<->unmuted per
    // talk; the camera keys its speaker off the unmute (mute->unmute) transition. We keep one
    // persistent published track and mute it after talk-back audio stops so the next talk unmutes
    // afresh — otherwise a permanently-unmuted track only triggers the camera the first time.
    //
    // There is ONE shared published mic track but potentially several consumer sessions (multiple
    // HomeKit devices). Only one may feed the track at a time: interleaving RTP from two sessions
    // (different SSRCs/timestamps) into the single sender corrupts the RED/sequence state and breaks
    // talk-back for everyone until the connection is torn down. `claimMic(token)` grants exclusive
    // ownership to the first talker; others are refused until the owner goes idle (inactivity).
    let micSid: string | undefined;
    let micMuted = false;
    let micOwner: object | undefined;
    let micMuteTimer: NodeJS.Timeout | undefined;
    const sendMute = (muted: boolean) => {
        if (!micSid)
            return;
        send({ case: 'mute', value: new MuteTrackRequest({ sid: micSid, muted }) });
        dlog('SS:LiveKit mic', muted ? 'muted (talk stop)' : 'unmuted (talk start)', micSid);
    };
    // Called on each inbound talk-back packet with the calling session's token. Returns true only if
    // this session owns the mic (and may write RTP). Unmutes on claim and arms an inactivity timer
    // that releases ownership + re-mutes shortly after audio stops.
    const claimMic = (token: object): boolean => {
        if (micOwner && micOwner !== token)
            return false;
        if (!micOwner) {
            micOwner = token;
            if (micMuted) {
                micMuted = false;
                sendMute(false);
            }
        }
        if (micMuteTimer)
            clearTimeout(micMuteTimer);
        micMuteTimer = setTimeout(() => {
            micOwner = undefined;
            micMuted = true;
            sendMute(true);
        }, 800);
        return true;
    };

    const cleanup = () => {
        if (pingTimer)
            clearInterval(pingTimer);
        if (statsTimer)
            clearInterval(statsTimer);
        if (micMuteTimer)
            clearTimeout(micMuteTimer);
        try { ws.close(); } catch { /* ignore */ }
        try { pc?.close(); } catch { /* ignore */ }
        try { publisherPc?.close(); } catch { /* ignore */ }
    };

    const send = (message: SignalRequest['message']) => {
        if (ws.readyState !== WebSocket.OPEN)
            return;
        ws.send(new SignalRequest({ message }).toBinary());
    };

    const setupPeerConnection = (iceServers: any[]) => {
        pc = new RTCPeerConnection({
            iceServers: normalizeIceServers(iceServers),
            bundlePolicy: 'max-bundle',
            codecs: {
                video: SUBSCRIBE_VIDEO_CODECS,
                audio: SUBSCRIBE_AUDIO_CODECS,
            },
        });

        pc.onIceCandidate.subscribe(candidate => {
            if (!candidate?.candidate)
                return;
            send({
                case: 'trickle',
                value: new TrickleRequest({
                    candidateInit: JSON.stringify(candidate.toJSON()),
                    target: SignalTarget.SUBSCRIBER,
                }),
            });
        });

        pc.onTrack.subscribe(track => {
            if (track.kind === 'video') {
                videoTrack = track;
                // Passively watch the video RTP to retain the latest keyframe for on-demand snapshots.
                track.onReceiveRtp.subscribe(rtp => {
                    videoStats.onPacket(rtp.header.sequenceNumber);
                    try { keyframeCapturer.onRtp(rtp); }
                    catch { /* ignore malformed packet */ }
                });
                // Observe werift's NACK handler so the stats show whether retransmission requests
                // are actually being sent for the gaps we see.
                try {
                    const receiver = (pc!.getTransceivers() as any[])
                        .find(t => t?.receiver?.tracks?.includes?.(track) || t?.kind === 'video')?.receiver;
                    if (receiver) {
                        dlog('SS:rtpstats video nackEnabled=', !!receiver.nackEnabled);
                        receiver.nack?.onPacketLost?.subscribe?.(() => { nackSends++; });
                    }
                }
                catch (e) {
                    dlog('SS:rtpstats receiver introspection failed', e);
                }
                startStatsLogging();
            }
            else if (track.kind === 'audio') {
                audioTrack = track;
                track.onReceiveRtp.subscribe(rtp => audioStats.onPacket(rtp.header.sequenceNumber));
                startStatsLogging();
            }
        });
    };

    await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('LiveKit signaling websocket open timed out.')), 15000);
        ws.on('open', () => { clearTimeout(timeout); dlog('SS:LiveKit ws open'); resolve(); });
        ws.on('error', err => { clearTimeout(timeout); reject(err); });
    });

    const handleSignal = async (data: WebSocket.RawData) => {
        try {
            const bytes = data instanceof Buffer ? new Uint8Array(data)
                : data instanceof ArrayBuffer ? new Uint8Array(data)
                : new Uint8Array(data as any);
            const resp = SignalResponse.fromBinary(bytes);
            const msg = resp.message;

            switch (msg.case) {
                case 'join': {
                    const join = msg.value;
                    dlog('SS:LiveKit join subscriberPrimary=', join.subscriberPrimary,
                        'iceServers=', join.iceServers?.length, 'pingInterval=', join.pingInterval);
                    joinIceServers = join.iceServers as any[];
                    setupPeerConnection(join.iceServers as any[]);
                    const intervalMs = (join.pingInterval || 30) * 1000;
                    pingTimer = setInterval(() => send({ case: 'ping', value: BigInt(Date.now()) }), intervalMs);
                    break;
                }
                case 'offer': {
                    if (!pc) {
                        console.warn('SS:LiveKit received offer before join; ignoring.');
                        break;
                    }
                    const offer = msg.value;
                    // Log the camera's offered H264 profiles so we can align werift's codec if the
                    // negotiation fails to match.
                    const h264 = (offer.sdp.match(/a=(rtpmap|fmtp):.*(H264|h264).*/g) || []).join(' | ');
                    if (h264)
                        dlog('SS:LiveKit server H264 offer:', h264);
                    // Loss diagnostics: does the server offer retransmission (rtx) / FEC, and does it
                    // pair ssrcs for it (ssrc-group FID)?
                    const resilience = (offer.sdp.match(/a=(rtpmap:\d+ (rtx|red|ulpfec|flexfec).*|fmtp:\d+ apt=\d+|ssrc-group:FID.*)/g) || []).join(' | ');
                    if (resilience)
                        dlog('SS:LiveKit server offer resilience:', resilience);
                    await pc.setRemoteDescription({ type: 'offer', sdp: offer.sdp });
                    remoteDescriptionSet = true;
                    for (const candidate of pendingCandidates.splice(0))
                        await pc.addIceCandidate(candidate).catch(() => { /* ignore */ });
                    const answer = await pc.createAnswer();
                    await pc.setLocalDescription(answer);
                    const answerVideo = (answer.sdp.match(/a=rtpmap:.*/g) || []).join(' | ');
                    if (answerVideo)
                        dlog('SS:LiveKit our answer rtpmap:', answerVideo);
                    send({
                        case: 'answer',
                        value: new SessionDescription({ type: 'answer', sdp: answer.sdp, id: offer.id }),
                    });
                    break;
                }
                case 'answer': {
                    // The server answers the offer we sent on the PUBLISHER peer connection when
                    // publishing the mic track. (The subscriber flow is the reverse: server offers.)
                    if (!publisherPc) {
                        console.warn('SS:LiveKit received answer with no publisher PC; ignoring.');
                        break;
                    }
                    const answer = msg.value;
                    await publisherPc.setRemoteDescription({ type: 'answer', sdp: answer.sdp });
                    publisherRemoteDescriptionSet = true;
                    for (const candidate of pendingPublisherCandidates.splice(0))
                        await publisherPc.addIceCandidate(candidate).catch(() => { /* ignore */ });
                    // werift derives the RED block payload type from the negotiated red fmtp, but
                    // LiveKit's answer leaves it empty -> redRedundantPayloadType=0 (falsy) -> the
                    // sender skips RED wrapping and ships raw Opus mislabelled as RED, which the camera
                    // can't decode. Force it to the Opus PT so werift actually builds RED packets.
                    if (publisherMicSender && !publisherMicSender.redRedundantPayloadType) {
                        publisherMicSender.redRedundantPayloadType = PUBLISH_AUDIO_CODECS[1].payloadType;
                        dlog('SS:LiveKit forced publisher RED redundant PT=', publisherMicSender.redRedundantPayloadType);
                    }
                    dlog('SS:LiveKit publisher answer applied.');
                    break;
                }
                case 'trickle': {
                    const init = JSON.parse(msg.value.candidateInit);
                    if (!init?.candidate)
                        break;
                    const candidate = new RTCIceCandidate({
                        candidate: init.candidate,
                        sdpMid: init.sdpMid,
                        sdpMLineIndex: init.sdpMLineIndex,
                    });
                    if (msg.value.target === SignalTarget.PUBLISHER) {
                        if (!publisherPc)
                            break;
                        if (publisherRemoteDescriptionSet)
                            await publisherPc.addIceCandidate(candidate).catch(() => { /* ignore */ });
                        else
                            pendingPublisherCandidates.push(candidate);
                        break;
                    }
                    if (!pc)
                        break;
                    if (remoteDescriptionSet)
                        await pc.addIceCandidate(candidate).catch(() => { /* ignore */ });
                    else
                        pendingCandidates.push(candidate);
                    break;
                }
                case 'trackPublished':
                    dlog('SS:LiveKit track published:', msg.value.cid, msg.value.track?.sid, msg.value.track?.type);
                    // Remember the mic track sid so we can mute/unmute it per talk session.
                    if (msg.value.track?.type === TrackType.AUDIO && msg.value.track?.sid)
                        micSid = msg.value.track.sid;
                    break;
                case 'leave':
                    dlog('SS:LiveKit server requested leave.');
                    cleanup();
                    break;
                default:
                    break;
            }
        }
        catch (err) {
            console.warn('Failed to handle LiveKit signaling message.', err);
        }
    };

    // Serialize signaling: LiveKit renegotiates by sending several offers (audio first, then
    // video). Processing them concurrently corrupts werift's signaling state and can drop the
    // video offer, so handle one message at a time.
    let signalQueue: Promise<void> = Promise.resolve();
    ws.on('message', (data: WebSocket.RawData) => {
        signalQueue = signalQueue.then(() => handleSignal(data));
    });

    ws.on('close', () => dlog('SS:LiveKit ws closed.'));

    // Wait for the subscriber peer connection to connect and deliver at least a video track.
    await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('LiveKit subscriber connection timed out.')), 30000);
        let settled = false;
        const finish = (err?: Error) => {
            if (settled)
                return;
            settled = true;
            clearTimeout(timeout);
            err ? reject(err) : resolve();
        };
        const check = () => {
            if (pc?.connectionState === 'connected' && videoTrack)
                finish();
        };
        const poll = setInterval(() => {
            if (settled) {
                clearInterval(poll);
                return;
            }
            if (pc) {
                pc.connectionStateChange.subscribe(state => {
                    if (state === 'failed' || state === 'closed')
                        finish(new Error(`LiveKit subscriber connection ${state}.`));
                    else
                        check();
                });
                pc.onTrack.subscribe(() => check());
                clearInterval(poll);
            }
        }, 100);
    });

    // A single, persistent talk-back (microphone) track is published to LiveKit at most once per
    // connection and shared by every consumer session. Publishing per-session would add a new
    // transceiver and renegotiate the shared publisher each time, which corrupts it after the first
    // session (talk-back "works once then stops").
    //
    // Every consumer session's talk-back RTP is written into this one shared sender. Each HomeKit
    // talk session has its own SSRC/sequence/timestamp bases, and werift's sender passes the input
    // sequence/timestamp through (only offsetting the SSRC), so switching sources would make the
    // sequence jump backwards and the camera would discard the packets as stale. `writeMic` rewrites
    // every packet's sequence/timestamp to our own monotonic counters so the stream stays continuous
    // across source switches, regardless of which session is talking.
    let micPublish: Promise<MediaStreamTrack> | undefined;
    let micTrackRef: MediaStreamTrack | undefined;
    let micSeq = Math.floor(Math.random() * 0xffff);
    let micTs = Math.floor(Math.random() * 0xffffffff);
    const writeMic = (rtp: RtpPacket): void => {
        if (!micTrackRef)
            return;
        rtp.header.sequenceNumber = micSeq;
        micSeq = (micSeq + 1) & 0xffff;
        rtp.header.timestamp = micTs;
        micTs = (micTs + 960) >>> 0; // 20ms @ 48kHz Opus
        micTrackRef.writeRtp(rtp);
    };
    const ensureMicTrack = (): Promise<MediaStreamTrack> => {
        if (micPublish)
            return micPublish;
        micPublish = (async () => {
            if (!publisherPc) {
                publisherPc = new RTCPeerConnection({
                    iceServers: normalizeIceServers(joinIceServers),
                    bundlePolicy: 'max-bundle',
                    codecs: { audio: PUBLISH_AUDIO_CODECS },
                });
                publisherPc.onIceCandidate.subscribe(candidate => {
                    if (!candidate?.candidate)
                        return;
                    send({
                        case: 'trickle',
                        value: new TrickleRequest({
                            candidateInit: JSON.stringify(candidate.toJSON()),
                            target: SignalTarget.PUBLISHER,
                        }),
                    });
                });
                publisherPc.connectionStateChange.subscribe(state =>
                    dlog('SS:LiveKit publisher connection state:', state));
            }

            const track = new MediaStreamTrack({ kind: 'audio', id: `mic-${Date.now()}` });
            micTrackRef = track;
            const micTransceiver = publisherPc.addTransceiver(track, { direction: 'sendonly' });
            // The 'answer' handler force-enables RED wrapping on this sender once negotiation settles
            // (werift otherwise leaves redRedundantPayloadType=0 and sends raw Opus mislabelled as RED).
            publisherMicSender = micTransceiver.sender;

            // Announce the track to LiveKit, then negotiate: we (the client) offer on the PUBLISHER
            // target and the server answers (handled in the 'answer' case above).
            const cid = track.id!;
            send({
                case: 'addTrack',
                value: new AddTrackRequest({
                    cid,
                    // Match the SimpliSafe app's talk-back track exactly (name "microphone"); the
                    // camera keys its speaker playback off this convention.
                    name: 'microphone',
                    type: TrackType.AUDIO,
                    source: TrackSource.MICROPHONE,
                }),
            });

            const offer = await publisherPc.createOffer();
            await publisherPc.setLocalDescription(offer);
            send({
                case: 'offer',
                value: new SessionDescription({ type: 'offer', sdp: offer.sdp }),
            });
            dlog('SS:LiveKit publishing mic track cid=', cid);
            return track;
        })();
        // Allow a retry on failure (e.g. transient publisher negotiation error).
        micPublish.catch(() => { micPublish = undefined; });
        return micPublish;
    };

    return { pc: pc!, videoTrack, audioTrack, close: cleanup, getKeyframe: () => keyframeCapturer.keyframe, ensureMicTrack, claimMic, writeMic };
}

/**
 * Minimal RTCSignalingSession wrapper around a werift peer connection, adapted from
 * @scrypted/webrtc's WeriftSignalingSession.
 */
class WeriftSignalingSession implements RTCSignalingSession {
    __proxy_props = { options: {} };
    options: RTCSignalingOptions = {};
    remoteDescription?: Promise<void>;

    constructor(public console: ConsoleLike, public pc: RTCPeerConnection) {
    }

    async getOptions(): Promise<RTCSignalingOptions> {
        return this.options;
    }

    async createLocalDescription(type: 'offer' | 'answer', setup: RTCAVSignalingSetup, sendIceCandidate: undefined | RTCSignalingSendIceCandidate): Promise<RTCSessionDescriptionInit> {
        this.pc.onIceCandidate.subscribe(candidate => {
            sendIceCandidate?.({
                candidate: candidate.candidate,
                sdpMid: candidate.sdpMid,
                sdpMLineIndex: candidate.sdpMLineIndex,
            });
        });

        if (type === 'offer') {
            const offer = await this.pc.createOffer();
            this.pc.setLocalDescription(offer);
            return { type: offer.type, sdp: offer.sdp };
        }

        if (!sendIceCandidate)
            await this.remoteDescription;
        const answer = await this.pc.createAnswer();
        this.pc.setLocalDescription(answer);
        return { type: answer.type, sdp: answer.sdp };
    }

    async setRemoteDescription(description: RTCSessionDescriptionInit, setup: RTCAVSignalingSetup): Promise<void> {
        this.remoteDescription = this.pc.setRemoteDescription(description as any);
        await this.remoteDescription;
    }

    async addIceCandidate(candidate: RTCIceCandidateInit): Promise<void> {
        await this.pc.addIceCandidate(new RTCIceCandidate(candidate as any));
    }
}

function createCandidateQueue(console: ConsoleLike, session: RTCSignalingSession) {
    let ready = false;
    let queue: RTCIceCandidateInit[] = [];
    const send = async (candidate: RTCIceCandidateInit) => {
        try {
            await session.addIceCandidate(candidate);
        }
        catch (e) {
            console.error('addIceCandidate error', e);
        }
    };
    return {
        flush() {
            ready = true;
            for (const candidate of queue)
                send(candidate);
            queue = [];
        },
        queueSendCandidate: async (candidate: RTCIceCandidateInit) => {
            if (!ready)
                queue.push(candidate);
            else
                send(candidate);
        },
    };
}

/**
 * Negotiate an offer/answer pair between two signaling clients. Adapted from
 * @scrypted/webrtc's connectRTCSignalingClients.
 */
async function connectRTCSignalingClients(
    console: ConsoleLike,
    offerClient: RTCSignalingSession,
    offerSetup: Partial<RTCAVSignalingSetup>,
    answerClient: RTCSignalingSession,
    answerSetup: Partial<RTCAVSignalingSetup>,
) {
    offerSetup.type = 'offer';
    answerSetup.type = 'answer';

    const answerQueue = createCandidateQueue(console, answerClient);
    const offerQueue = createCandidateQueue(console, offerClient);

    const offer = await offerClient.createLocalDescription('offer', offerSetup as RTCAVSignalingSetup, answerQueue.queueSendCandidate);
    await answerClient.setRemoteDescription(offer, answerSetup as RTCAVSignalingSetup);
    const answer = await answerClient.createLocalDescription('answer', answerSetup as RTCAVSignalingSetup, offerQueue.queueSendCandidate);
    await offerClient.setRemoteDescription(answer, offerSetup as RTCAVSignalingSetup);
    offerQueue.flush();
    answerQueue.flush();
}

/**
 * Manages a single shared LiveKit subscriber connection per camera and fans it out to multiple
 * Scrypted consumers.
 *
 * Scrypted requests a stream several times concurrently (console preview, rebroadcast, NVR, etc.),
 * each calling startRTCSignalingSession. If each opened its own LiveKit connection, they would all
 * join the same room with the SAME participant identity (from the user's token) — and LiveKit
 * disconnects the existing participant whenever another joins with a duplicate identity, so the
 * connections kick each other out (intermittent "connection failed" / black screen). Instead we
 * keep one upstream subscriber and bridge every consumer off its tracks, tearing the upstream down
 * only when the last consumer leaves.
 */
export class LiveKitCameraStream {
    private viewer?: LiveKitViewer;
    private viewerPromise?: Promise<LiveKitViewer>;
    private refcount = 0;

    constructor(
        private getDetails: () => Promise<LiveKitDetails>,
        private console: ConsoleLike,
        private debug: () => boolean,
    ) {
    }

    private dlog(...args: any[]): void {
        if (this.debug())
            this.console.log(...args);
    }

    private viewerHealthy(viewer: LiveKitViewer): boolean {
        const state = viewer.pc.connectionState;
        return state !== 'failed' && state !== 'closed';
    }

    /**
     * The latest H264 keyframe (Annex-B) from the active viewer, if one is currently connected.
     * Returns undefined when the camera is idle (no live video is flowing).
     */
    getKeyframe(): Buffer | undefined {
        return this.viewer?.getKeyframe();
    }

    private async ensureViewer(): Promise<LiveKitViewer> {
        if (this.viewer && this.viewerHealthy(this.viewer))
            return this.viewer;

        if (!this.viewerPromise) {
            this.viewerPromise = (async () => {
                const details = await this.getDetails();
                const viewer = await startLiveKitViewer(details, this.console, this.debug);
                this.dlog('SS:LiveKit subscriber connected; bridging to Scrypted.',
                    'video=', viewer.videoTrack?.codec?.mimeType ?? false,
                    'audio=', viewer.audioTrack?.codec?.mimeType ?? false);
                this.viewer = viewer;
                viewer.pc.connectionStateChange.subscribe(state => {
                    if ((state === 'failed' || state === 'closed') && this.viewer === viewer) {
                        this.viewer = undefined;
                        this.viewerPromise = undefined;
                    }
                });
                return viewer;
            })();
            this.viewerPromise.catch(() => { this.viewerPromise = undefined; });
        }
        return this.viewerPromise;
    }

    async startSession(session: RTCSignalingSession): Promise<RTCSessionControl> {
        const viewer = await this.ensureViewer();
        this.refcount++;
        try {
            return await bridgeToScryptedSession(session, viewer, this.console, () => this.release());
        }
        catch (err) {
            this.release();
            throw err;
        }
    }

    /**
     * Acquire the shared talk-back sink for the device-level Intercom path (used by non-WebRTC
     * consumers). Ensures a connection exists (so talk-back works even if nothing is actively
     * viewing) and holds a refcount for the duration. Returns the shared mic track to write RTP
     * into, plus a release function that drops the refcount.
     */
    async acquireMicSink(): Promise<{ track: MediaStreamTrack; release: () => void }> {
        const viewer = await this.ensureViewer();
        this.refcount++;
        let released = false;
        const release = () => {
            if (released)
                return;
            released = true;
            this.release();
        };
        try {
            const track = await viewer.ensureMicTrack();
            return { track, release };
        }
        catch (err) {
            release();
            throw err;
        }
    }

    private release(): void {
        this.refcount = Math.max(0, this.refcount - 1);
        if (this.refcount === 0) {
            this.viewer?.close();
            this.viewer = undefined;
            this.viewerPromise = undefined;
        }
    }
}

async function bridgeToScryptedSession(
    session: RTCSignalingSession,
    viewer: LiveKitViewer,
    console: ConsoleLike,
    onEnd: () => void,
): Promise<RTCSessionControl> {

    // Match the consumer peer connection's codecs to what LiveKit negotiated so RTP can be
    // forwarded without transcoding (payload types line up).
    const videoCodec = viewer.videoTrack?.codec;
    const audioCodec = viewer.audioTrack?.codec;
    const codecs: { video?: RTCRtpCodecParameters[]; audio?: RTCRtpCodecParameters[] } = {};
    if (videoCodec)
        codecs.video = [videoCodec];
    if (audioCodec)
        codecs.audio = [audioCodec];

    const consumerPc = new RTCPeerConnection({
        bundlePolicy: 'max-bundle',
        codecs,
    });

    // Outbound video, fed by forwarding RTP from the LiveKit track.
    const videoOut = new MediaStreamTrack({ kind: 'video' });
    consumerPc.addTransceiver(videoOut, { direction: 'sendonly' });
    viewer.videoTrack?.onReceiveRtp.subscribe(rtp => videoOut.writeRtp(rtp));

    // Audio is bidirectional. We send the camera's audio to the consumer AND accept talk-back (the
    // viewer's microphone) on the same transceiver: Scrypted's WebRTC plugin implements HomeKit
    // two-way audio by pushing the mic audio back over THIS peer connection (it requests sendrecv),
    // not by calling the device's Intercom. Any inbound audio is forwarded up to the LiveKit
    // publisher so it reaches the camera.
    if (viewer.audioTrack) {
        const audioOut = new MediaStreamTrack({ kind: 'audio' });
        consumerPc.addTransceiver(audioOut, { direction: 'sendrecv' });
        viewer.audioTrack.onReceiveRtp.subscribe(rtp => audioOut.writeRtp(rtp));
    }
    else {
        consumerPc.addTransceiver('audio', { direction: 'recvonly' });
    }

    // Forward inbound talk-back audio into the shared LiveKit mic track. The track is published once
    // (lazily, on first audio) and reused across every session, so re-publishing/renegotiation never
    // happens — that was corrupting the shared publisher after the first talk-back. Only the session
    // that currently owns the mic (claimMic) writes RTP: mixing two sessions into the one sender
    // corrupts it and breaks talk-back for everyone.
    let micStarted = false;
    let micStarting = false;
    const micToken = {};
    consumerPc.onTrack.subscribe(track => {
        if (track.kind !== 'audio')
            return;
        track.onReceiveRtp.subscribe(rtp => {
            // Take (or keep) exclusive ownership of the shared mic; bail if another session is talking.
            if (!viewer.claimMic(micToken))
                return;
            if (!micStarting) {
                micStarting = true;
                viewer.ensureMicTrack()
                    .then(() => { micStarted = true; })
                    .catch(err => console.warn('SS:LiveKit talk-back publish failed.', err));
            }
            if (!micStarted)
                return;
            // Forward into the shared mic track with monotonic sequence/timestamp rewriting.
            viewer.writeMic(rtp);
        });
    });

    const weriftSession = new WeriftSignalingSession(console, consumerPc);
    const consumerSetup: Partial<RTCAVSignalingSetup> = {
        audio: { direction: 'sendrecv' },
        video: { direction: 'sendonly' },
    };
    const sessionSetup: Partial<RTCAVSignalingSetup> = {
        audio: { direction: 'sendrecv' },
        video: { direction: 'recvonly' },
    };

    // If the Scrypted consumer supplies its own offer, it must be the offering side; otherwise we
    // (werift) offer and the consumer answers.
    const options = session.options ?? await session.getOptions?.();
    if (options?.offer)
        await connectRTCSignalingClients(console, session, sessionSetup, weriftSession, consumerSetup);
    else
        await connectRTCSignalingClients(console, weriftSession, consumerSetup, session, sessionSetup);

    const control = new SimplisafeRTCSessionControl(() => {
        try { consumerPc.close(); } catch { /* ignore */ }
        // Release this consumer's hold on the shared LiveKit connection; the streamer tears the
        // upstream down only when the last consumer leaves. The shared mic track is not torn down
        // per session — it lives with the viewer and is closed when the last consumer leaves.
        onEnd();
    });

    consumerPc.connectionStateChange.subscribe(state => {
        if (state === 'failed' || state === 'closed')
            control.endSession().catch(() => { /* ignore */ });
    });
    viewer.pc.connectionStateChange.subscribe(state => {
        if (state === 'failed' || state === 'closed')
            control.endSession().catch(() => { /* ignore */ });
    });

    return control;
}

/**
 * Returned to Scrypted's WebRTC plugin. Must be a class instance (not a plain object literal) so
 * Scrypted's RPC passes it by reference/proxy instead of trying to structured-clone its methods.
 */
class SimplisafeRTCSessionControl implements RTCSessionControl {
    private ended = false;

    constructor(private teardown: () => void) {
    }

    async getRefreshAt(): Promise<number | void> {
        // No forced refresh. LiveKit only validates the access token at join time; a connected
        // participant is not disconnected when the token expires. Forcing a refresh would mean
        // reconnecting the shared upstream, which would interrupt every current viewer. If the
        // upstream ever does drop, the shared connection self-heals on the next stream request
        // (each reconnect fetches a fresh token), so no periodic refresh is needed.
        return undefined;
    }

    async extendSession(): Promise<void> {
    }

    async setPlayback(): Promise<void> {
    }

    async endSession(): Promise<void> {
        if (this.ended)
            return;
        this.ended = true;
        this.teardown();
    }
}
