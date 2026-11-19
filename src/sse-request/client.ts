//import { EventEmitter } from 'node:events';
import { EventEmitter } from '../browser-compatible/events.js';
import {createEventSource, EventSourceClient} from 'eventsource-client';
import {
    ConnectionId,
    RequestId,
    SSEEventType,
    PingPayload,
    ConnectionIdPayload,
    SSERequestEventType,
    RequestContent,
    RequestResponse,
    RequestEnvelope,
    RequestAckData,
    SSEResponseMsg,
    NotifyPayload
} from './types.js';
import {error_message} from './utils.js';

export type ClientState = "disconnected" | "connected" | "ready";

interface ReadyTrackerEvents {
    "disconnected": [];
    "connected": [];
    "ready": [];
    "state-change": [newState: ClientState];
    "connection-id-change": [oldConnId: ConnectionId, newConnId: ConnectionId];
}

class ReadyTracker extends EventEmitter {
    private _connected: boolean = false;
    private _connId: ConnectionId | null = null;
    constructor() {
        super();
    }
    on<K extends keyof ReadyTrackerEvents>(
        event: K,
        listener: (...args: ReadyTrackerEvents[K]) => void
    ): this {
        return super.on(event, listener);
    }
    emit<K extends keyof ReadyTrackerEvents>(
        event: K,
        ...args: ReadyTrackerEvents[K]
    ): boolean {
        return super.emit(event, ...args);
    }
    get state(): ClientState {
        if (!this._connected) {
            return "disconnected";
        } else if (this._connId) {
            return "ready";
        } else {
            return "connected";
        }
    }
    get connId(): ConnectionId | null {
        return this._connId;
    }
    get isReady(): boolean {
        return this.state === "ready";
    }
    onConnected(): void {
        if (this.state === "disconnected") {
            this._connected = true;
            this.emit("connected");
            this.emit("state-change", this.state);
        }
    }
    onReceiveConnectionId(connId: ConnectionId): void {
        if (!connId) {
            throw new Error("Connection id should be non-empty");
        }
        if (this.state === "connected") {
            this._connId = connId;
            this.emit("ready");
            this.emit("state-change", this.state);
        } else if (this.state === "ready") {
            const oldConnId = this._connId!;
            // This case can happen when the server sends a new connection id after the client is already ready, we just update the connection id.
            if (oldConnId !== connId) {
                this._connId = connId;
                this.emit("connection-id-change", oldConnId, connId);
            }
        }
    }
    reset(): void {
        if (this.state !== "disconnected") {
            this._connected = false;
            this._connId = null;
            this.emit("disconnected");
            this.emit("state-change", this.state);
        }
    }
}

export type RequestProcessor = (request: RequestContent) => Promise<RequestResponse>;

export interface ClientEvents {
    "state-change": [newState: ClientState];
    "ready": [];
    "connection-id-change": [oldConnId: ConnectionId, newConnId: ConnectionId];
    "change": [];
    "message": [data: any, event?: string, id?: string];    // emitted when a message is received from the SSE server
    "connection-id": [connId: ConnectionId];    // emitted when a <connection-id> message is received from the SSE server
    "ping": [timestamp: number];    // emitted when a <ping> message is received from the SSE server
    "request": [connId: ConnectionId, reqId: RequestId, content: RequestContent];   // emitted when a <request> message is received from the SSE server
    "request-ack-error": [connId: ConnectionId, reqId: RequestId, error: string];
    "request-acked": [connId: ConnectionId, reqId: RequestId, response: RequestResponse, reply: any];
    "event-notify": [event: string, data?: any];    // emitted when a <notify> message is received from the SSE server
}

const defaultRequestProcessor: RequestProcessor = async (request: RequestContent) => {
    return Promise.resolve({status: 200, body: {}, headers: {} } as RequestResponse);
};

export class Client extends EventEmitter {
    private _readyTracker: ReadyTracker = new ReadyTracker();
    private _readyPromise: Promise<void>; 
    private _eventSource: EventSourceClient | null = null;
    constructor(private sseUrl: string, private _requestProcessor: RequestProcessor = defaultRequestProcessor) {
        super();
        if (!sseUrl) {
            throw new Error("SSE URL should be non-empty");
        }
        this.initReadyTracker();
        this._readyPromise = this.createReadyPromise();
    }
    private initReadyTracker(): void {
        const me = this;
        this._readyTracker.reset();
        this._readyTracker.removeAllListeners();
        this._readyTracker.on("state-change", (newState) => {
            me.emit("state-change", newState);
            me.emit("change");
        }).on("connection-id-change", (oldConnId, newConnId) => {
            me.emit("connection-id-change", oldConnId, newConnId);
            me.emit("change");
        }).on("ready", () => {
            me.emit("ready");
        });
    }
    on<K extends keyof ClientEvents>(
        event: K,
        listener: (...args: ClientEvents[K]) => void
    ): this {
        return super.on(event, listener);
    }
    emit<K extends keyof ClientEvents>(
        event: K,
        ...args: ClientEvents[K]
    ): boolean {
        return super.emit(event, ...args);
    }
    private createReadyPromise(): Promise<void> {
        const me = this;
        return new Promise<void>((resolve) => {
            me._readyTracker.on("ready", () => {
                resolve();
            });
        });
    }
    connect(): Promise<void> {
        const me = this;
        this._eventSource = createEventSource({
            url: this.url,
            onMessage: ({data, event, id}) => {
                const o = JSON.parse(data);
                me.emit("message", o, event, id);
                const eventType = event as SSEEventType;
                if (eventType === "connection-id") {
                    const { connId } = (o as ConnectionIdPayload);
                    me.emit("connection-id", connId);
                    me._readyTracker.onReceiveConnectionId(connId);
                } else if (eventType === "ping") {
                    const { timestamp } = (o as PingPayload);
                    me.emit("ping", timestamp);
                } else if (eventType === "request") {
                    me.handleRequest(o as RequestEnvelope).then(() => {}).catch((_) => {});
                } else if (eventType === "notify") {
                    const { event, data } = (o as NotifyPayload);
                    me.emit("event-notify", event, data);
                }
            }
            ,onConnect:() => {
                me._readyTracker.onConnected();
            }
            ,onDisconnect:() => {
                me.close();
            }
        });
        return this.ready;
    }
    get url(): string {
        return this.sseUrl;
    }
    get state(): ClientState {
        return this._readyTracker.state;
    }
    get isReady(): boolean {
        return this._readyTracker.isReady;
    }
    get connId(): ConnectionId | null {
        return this._readyTracker.connId;
    }
    // the ready promise
    get ready(): Promise<void> {
        return this._readyPromise;
    }
    // send message back to the server by POSTing to the same URL.
    // throws Error()
    async sendMessage(event: string, data?: any) {
        await this.ready;
        try {
            const connId = this.connId!;
            const msg: SSEResponseMsg = { connId, event, data };
            const response = await fetch(this.url, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(msg) // Must stringify the JS object
            });
            if (!response.ok) {
                throw `Failed to send message. Status: ${response.status}, StatusText: ${response.statusText}`;
            }
            const reply = await response.json();
            return reply;
        } catch(err) {
            const message = error_message(err);
            throw new Error(message);
        }
    }
    // throws Error()
    async ackRequest(reqId: RequestId, response: RequestResponse) {
        const ackData: RequestAckData = { reqId, response };
        const event: SSERequestEventType = 'request';
        return await this.sendMessage(event, ackData);
    }
    private async handleRequest(requestEnvelope: RequestEnvelope) {
        await this.ready;
        const {reqId, content} = requestEnvelope;
        this.emit("request", this.connId!, reqId, content);
        let res: RequestResponse | null = null;
        try {
            res = await this._requestProcessor(content)
        } catch(error) {
            const message = error_message(error, "Internal server error"); 
            res = {
                status: 500,
                headers: {"content-type": "application/json"},
                body: {message}
            };
        }
        const response = res!;
        try {
            const reply = await this.ackRequest(reqId, response);
            this.emit("request-acked", this.connId!, reqId, response, reply);
        } catch(err) {
            const error = error_message(err);
            this.emit("request-ack-error", this.connId!, reqId, error);
        }
    }
    close(): void {
        if (this._eventSource) {
            try {this._eventSource.close();} catch (_) {}
            this._eventSource = null;
        }
        this.initReadyTracker();
        this._readyPromise = this.createReadyPromise();
    }
}