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
];
const SUBSCRIBE_AUDIO_CODECS = [
    new RTCRtpCodecParameters({
        mimeType: 'audio/opus',
        clockRate: 48000,
        channels: 2,
        payloadType: 111,
    }),
];
import {
    SessionDescription,
    SignalRequest,
    SignalResponse,
    SignalTarget,
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

interface LiveKitViewer {
    pc: RTCPeerConnection;
    videoTrack?: MediaStreamTrack;
    audioTrack?: MediaStreamTrack;
    close: () => void;
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
    let pingTimer: NodeJS.Timeout | undefined;
    let remoteDescriptionSet = false;
    const pendingCandidates: RTCIceCandidate[] = [];

    const cleanup = () => {
        if (pingTimer)
            clearInterval(pingTimer);
        try { ws.close(); } catch { /* ignore */ }
        try { pc?.close(); } catch { /* ignore */ }
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
            if (track.kind === 'video')
                videoTrack = track;
            else if (track.kind === 'audio')
                audioTrack = track;
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
                    await pc.setRemoteDescription({ type: 'offer', sdp: offer.sdp });
                    remoteDescriptionSet = true;
                    for (const candidate of pendingCandidates.splice(0))
                        await pc.addIceCandidate(candidate).catch(() => { /* ignore */ });
                    const answer = await pc.createAnswer();
                    await pc.setLocalDescription(answer);
                    send({
                        case: 'answer',
                        value: new SessionDescription({ type: 'answer', sdp: answer.sdp, id: offer.id }),
                    });
                    break;
                }
                case 'trickle': {
                    if (!pc)
                        break;
                    const init = JSON.parse(msg.value.candidateInit);
                    if (!init?.candidate)
                        break;
                    const candidate = new RTCIceCandidate({
                        candidate: init.candidate,
                        sdpMid: init.sdpMid,
                        sdpMLineIndex: init.sdpMLineIndex,
                    });
                    if (remoteDescriptionSet)
                        await pc.addIceCandidate(candidate).catch(() => { /* ignore */ });
                    else
                        pendingCandidates.push(candidate);
                    break;
                }
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

    return { pc: pc!, videoTrack, audioTrack, close: cleanup };
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

    // Outbound tracks fed by forwarding RTP from the LiveKit tracks.
    const videoOut = new MediaStreamTrack({ kind: 'video' });
    consumerPc.addTransceiver(videoOut, { direction: 'sendonly' });
    viewer.videoTrack?.onReceiveRtp.subscribe(rtp => videoOut.writeRtp(rtp));

    let audioOut: MediaStreamTrack | undefined;
    if (viewer.audioTrack) {
        audioOut = new MediaStreamTrack({ kind: 'audio' });
        consumerPc.addTransceiver(audioOut, { direction: 'sendonly' });
        viewer.audioTrack.onReceiveRtp.subscribe(rtp => audioOut!.writeRtp(rtp));
    }

    const weriftSession = new WeriftSignalingSession(console, consumerPc);
    const consumerSetup: Partial<RTCAVSignalingSetup> = {
        audio: { direction: 'sendonly' },
        video: { direction: 'sendonly' },
    };
    const sessionSetup: Partial<RTCAVSignalingSetup> = {
        audio: { direction: 'recvonly' },
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
        // upstream down only when the last consumer leaves.
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
