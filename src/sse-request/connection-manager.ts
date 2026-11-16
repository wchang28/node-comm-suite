import { EventEmitter } from 'events';
import express, { Request, Response, Router } from 'express';
import {generateSecureToken, error_message} from './utils.js';
import assert from 'assert';
import {
    ConnectionId,
    RequestId,
    RequestContent,
    RequestEnvelope,
    SSEResponseMsg,
    RequestResponse,
    RequestAckEnvelope,
    SSEEventType,
    PingPayload,
    ConnectionIdPayload,
    RequestError,
    ConnectionManagerError,
    NotifyPayload
} from './types.js';

interface RequestTrackerEvents {
    "completed": [];
}

class RequestTracker extends EventEmitter {
    private _pending: boolean = true;
    private _ackTimeoutHandle: NodeJS.Timeout | null = null;
    private _startTime: number = Date.now();
    private _response: RequestResponse | null = null;
    private _error: RequestError | null = null;
    private _endTime: number | null = null;
    private _durationMS: number | null = null;
    constructor(public reqId: RequestId, public path: string, timeoutMS: number = 30000) {
        super();
        const me = this;
        this._ackTimeoutHandle = setTimeout(() => {
            me._ackTimeoutHandle = null;
            me.onCompleted(null, 'ack-timeout');
        }, timeoutMS); // Ack timeout of 30 seconds
    }
    once<K extends keyof RequestTrackerEvents>(
        event: K,
        listener: (...args: RequestTrackerEvents[K]) => void
    ): this {
        return super.once(event, listener);
    }
    emit<K extends keyof RequestTrackerEvents>(
        event: K,
        ...args: RequestTrackerEvents[K]
    ): boolean {
        return super.emit(event, ...args);
    }
    get pending(): boolean {
        return this._pending;
    }
    get completed(): boolean {
        return !this._pending;
    }
    get error(): RequestError | null {
        return this._error;
    }
    get response(): RequestResponse | null {
        return this._response;
    }
    get success(): boolean {
        assert(this._pending === false, 'Cannot determine success of a pending dispatch');
        return (this._error === null);
    }
    get failed(): boolean {
        assert(this._pending === false, 'Cannot determine failure of a pending dispatch');
        return (this._error !== null);
    }
    get startTime(): number {
        return this._startTime!;
    }
    get acked(): boolean {
        return (!this._pending && this._error === null);
    }
    get endTime(): number | null {
        return this._endTime;
    }
    get durationMS(): number | null {
        return this._durationMS;
    }
    private onCompleted(response: RequestResponse | null, error: RequestError | null) {
        if (this._pending) {
            this._pending = false;
            this._response = response;
            this._error = error;
            if (this._ackTimeoutHandle !== null) {  // kill the ack timeout timer if still running
                clearTimeout(this._ackTimeoutHandle);
                this._ackTimeoutHandle = null;
            }
            this._endTime = Date.now();
            this._durationMS = Math.max(0, this._endTime - (this._startTime));
            this.emit('completed');
        }
    }
    onAcked(response: RequestResponse): void {
        this.onCompleted(response, null);
    }
    onConnectionClosed() {
        this.onCompleted(null, 'disconnected');
    }
}

interface ConnectionEvents {
    "message": [event: string, data?: any]; // fired when this connection receives a message notification
}

export class Connection extends EventEmitter {
    private _pingInterval: NodeJS.Timeout | null = null;
    private _remoteAddress: string | null = null;
    private _remotePort: number | null = null;
    private _requestTrackers: Map<RequestId, RequestTracker> = new Map<RequestId, RequestTracker>();
    constructor(public connId: ConnectionId, req: Request, private _response: Response, pingIntervalMS: number = 30000, public requestTimeoutMS: number = 30000) {
        super();
        this._remoteAddress = req.socket.remoteAddress || null;
        this._remotePort = req.socket.remotePort || null;
        const me = this;
        this._pingInterval = setInterval(() => {
            me.sendPing();
        }, pingIntervalMS);
    }
    on<K extends keyof ConnectionEvents>(
        event: K,
        listener: (...args: ConnectionEvents[K]) => void
    ): this {
        return super.on(event, listener);
    }
    emit<K extends keyof ConnectionEvents>(
        event: K,
        ...args: ConnectionEvents[K]
    ): boolean {
        return super.emit(event, ...args);
    }
    sendEventMessage(event: SSEEventType, data: any): void {
        const message = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
        this._response.write(message);
    }
    // send a request message over the SSE connection and return request id
    private sendRequest(path: string, method: string = "GET", body?: any, headers?: {[key: string]: string}): RequestId {
        const reqId = generateSecureToken(10);
        const content: RequestContent = { path, method, body, headers };
        const reqEnvelope: RequestEnvelope = { reqId, content };
        this.sendEventMessage('request', reqEnvelope);
        return reqId;
    }
    // send the assigned connection ID to the SSE client
    sendConnectionId(): void {
        const payload: ConnectionIdPayload = { connId: this.connId };
        this.sendEventMessage('connection-id', payload);
    }
    // send a PING message to the SSE client
    private sendPing(): void {
        const payload: PingPayload = { timestamp: Date.now() }; 
        this.sendEventMessage('ping', payload);
    }
    // send a event notification message to the SSE client
    sendNotification(event: string, data?: any): void {
        const payload: NotifyPayload = { event, data };
        this.sendEventMessage('notify', payload);
    }
    request_raw(path: string, method: string = "GET", body?: any, headers?: {[key: string]: string}): Promise<RequestResponse> {
        const reqId = this.sendRequest(path, method, body, headers);    // send the request and get the generated request ID
        const tracker = new RequestTracker(reqId, path, this.requestTimeoutMS);
        this._requestTrackers.set(reqId, tracker);
        const me = this;
        return new Promise<RequestResponse>((resolve) => {
            tracker.once('completed', () => {
                tracker.removeAllListeners();
                me._requestTrackers.delete(reqId);
                let response: RequestResponse = {status: 500, headers: {"content-type": "application/json"}, body: {}};
                if (tracker.success && tracker.response) {
                    response = tracker.response;
                } else {
                    const message = (tracker.error ? tracker.error as string : "unknown-error");
                    response.body.message = message;
                }
                resolve(response);
            });
        });
    }
    // make a request over the SSE connection
    // throws an Error object if the request fails or times out, otherwise returns a promise that resolves to the response body
    async request<T>(path: string, method: string = "GET", body?: any, headers?: {[key: string]: string}) {
        const response = await this.request_raw(path, method, body, headers);
        if (response.status >= 200 && response.status < 300) {
            return response.body as T;
        } else {
            const cause = `Request failed with status code ${response.status}`;
            const message = error_message(response.body, "unknown-error");
            throw new Error(message, { cause });
        }
    }
    onRequestAcked(reqId: RequestId, response: RequestResponse): void {
        const tracker = this._requestTrackers.get(reqId);
        if (tracker) {
            tracker.onAcked(response);
        }
    }
    // this connection received a normal message (not a request ack)
    onMessageReceived(event: string, data?: any): void {
        this.emit("message", event, data);
    }
    // cleanup on client-initiated disconnect
    onClientDisconnect(): void {
        // stop ping interval
        if (this._pingInterval !== null) {
            clearInterval(this._pingInterval);
            this._pingInterval = null;
        }
        if (this._requestTrackers.size > 0) {   // there are pending requests, notify them of connection closure
            const trackers = Array.from(this._requestTrackers.values());
            for (const tracker of trackers) {
                tracker.onConnectionClosed();
            }
            this._requestTrackers.clear();
        }
        this.removeAllListeners();   // remove all event listeners for this connection
    }
    get numPendingRequests(): number {
        return this._requestTrackers.size;
    }
    get remoteAddress(): string | null {
        return this._remoteAddress;
    }   
    get remotePort(): number | null {
        return this._remotePort;
    }
    get remoteSocketAddress(): string {
        const address = this._remoteAddress || '?';
        const port = this._remotePort !== null ? `${this._remotePort}` : '?';
        return `${address}:${port}`;
    }
}

interface ConnectionManagerEvents {
    "connected": [connection: Connection];
    "disconnected": [connection: Connection];
    "connection-rejected": [remoteAddress: string|null, remotePort: number|null];
}

export class ConnectionManager extends EventEmitter {
    private _connections: Map<ConnectionId, Connection> = new Map<ConnectionId, Connection>();
    private _connectionLists: Connection[] = [];
    constructor(
        public pingIntervalMS: number = 30000,
        public requestTimeoutMS: number = 30000,
        public maxAllowedConnections: number|null = null
    ) {
        super();
    }
    on<K extends keyof ConnectionManagerEvents>(
        event: K,
        listener: (...args: ConnectionManagerEvents[K]) => void
    ): this {
        return super.on(event, listener);
    }
    emit<K extends keyof ConnectionManagerEvents>(
        event: K,
        ...args: ConnectionManagerEvents[K]
    ): boolean {
        return super.emit(event, ...args);
    }
    onConnectionRequestAcked(connId: ConnectionId, reqId: RequestId, response: RequestResponse): void {
        const connection = this._connections.get(connId);
        if (connection) {
            connection.onRequestAcked(reqId, response);
        }
    }
    private onClientDisconnect(connId: ConnectionId): void {
        const connection = this._connections.get(connId);
        if (connection) {
            connection.onClientDisconnect();
            this._connections.delete(connId);
            const index = this._connectionLists.indexOf(connection);
            if (index >= 0) {
                this._connectionLists.splice(index, 1);
                this.emit('disconnected', connection);
            }
        }
    }
    private addConnectionImpl(req: Request, res: Response): Connection {
        const connId = generateSecureToken(10); // generate a unique connection ID
        const connection = new Connection(connId, req, res, this.pingIntervalMS, this.requestTimeoutMS);
        this._connections.set(connId, connection);
        this._connectionLists.push(connection);
        return connection;
    }
    // handling a non-request/response message from the client
    private onMessageReceived(msg: SSEResponseMsg): void {
        const connection = this._connections.get(msg.connId);
        if (connection) {
            connection.onMessageReceived(msg.event, msg.data);
        }
    }
    connection(connId: ConnectionId): Connection | null {
        const connection = this._connections.get(connId);
        return connection || null;
    }
    getConnection(connId: ConnectionId): Connection {
        const connection = this.connection(connId);
        if (!connection) {
            const message: ConnectionManagerError = "bad-connection-id";
            throw new Error(message, { cause: `Connection with ID ${connId} not found` });
        }
        return connection;
    }
    // send a notification message to a specific SSE client
    sendConnectionNotification(connId: ConnectionId, event: string, data?: any): void {
        const connection = this._connections.get(connId);
        if (connection) {
            connection.sendNotification(event, data);
        }
    }
    // broadcast a notification message to all connected SSE clients
    broadcastNotification(event: string, data?: any): void {
        for (const connection of this._connectionLists) {
            connection.sendNotification(event, data);
        }
    }
    setupExpressRouter(router: Router, path: string = "/events"): void {
        const me = this;
        // set the SSE endpoint for client to establish SSE connection
        router.get(path, (req: Request, res: Response) => {
            // server getting a SSE connection request from the client
            if (me.maxAllowedConnections == null || me.numConnections < me.maxAllowedConnections) {
                // set headers for SSE
                res.setHeader('Content-Type', 'text/event-stream');
                res.setHeader('Cache-Control', 'no-cache, no-transform');
                res.setHeader('Connection', 'keep-alive');
                // Explicitly flush headers immediately (important if compression middleware is used)
                res.flushHeaders(); 
                const connection = me.addConnectionImpl(req, res);
                // send the connection id to client so the client can use it later for sending message back
                connection.sendConnectionId();
                // handle client disconnect
                req.on('close', () => {
                    me.onClientDisconnect(connection.connId);
                });
                me.emit('connected', connection);
            } else {
                // Instantly destroys the socket connection
                const remoteAddress = req.socket?.remoteAddress || null;
                const remotePort = req.socket?.remotePort || null;
                res.socket?.destroy();
                me.emit("connection-rejected", remoteAddress, remotePort);
            }
        });
        // set the endpoint for client to send message back to server via http POST request
        router.post(path, express.json({limit: '999mb'}), (req: Request, res: Response) => {
            const msg: SSEResponseMsg = req.body;
            if (msg.event === 'request') {  // handling a request ack message
                const envelope: RequestAckEnvelope = req.body;
                me.onConnectionRequestAcked(envelope.connId, envelope.data.reqId, envelope.data.response);
            } else {    // handle a normal (not a request ack) message
                me.onMessageReceived(msg);
            }
            res.json({});
        });
    }
    get numConnections(): number {
        return this._connectionLists.length;
    }
    get connectionLists(): Connection[] {
        return this._connectionLists;
    }
}