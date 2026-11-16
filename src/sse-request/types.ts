export type ConnectionId = string;
export type RequestId = string;

export type RequestError = 'ack-timeout' | 'disconnected';
export type ConnectionManagerError = "bad-connection-id";

export interface RequestContent {
    path: string;
    method: string;
    body?: any;
    headers?: {[key: string]: string};
}

export interface RequestEnvelope {
    reqId: RequestId;
    content: RequestContent;
} 

export interface SSEResponseMsg {
    connId: ConnectionId;
    event: string;
    data?: any;
};

export type SSERequestEventType = 'request';

export interface RequestResponse {
    status: number;
    body: any;
    headers: {[key: string]: string};
}

export interface RequestAckData {
    reqId: RequestId;
    response: RequestResponse;
}

export interface RequestAckEnvelope {
    connId: ConnectionId;
    event: SSERequestEventType;
    data: RequestAckData;
}

export type SSEEventType = SSERequestEventType | 'ping' | 'connection-id' | 'notify';

export interface ConnectionIdPayload {
    connId: ConnectionId;
}

export interface PingPayload {
    timestamp: number;
}

export interface NotifyPayload {
    event: string;  // notification event name
    data?: any;     // optional notification data
}