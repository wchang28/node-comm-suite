import {generateSecureToken} from "./utils.js";

interface SSERequestTracker {
    connId: string;
    requestId: string;
    once: (event: 'completed', listener: (data: any) => void) => this;
}

interface ISSERequestsMonitor {
    sendMessage(connId: string, event: string, data?: any): void;
    createRequestTracker(connId: string, requestId: string): SSERequestTracker;
    getRequestTracker(connId: string, requestId: string): SSERequestTracker | undefined;
}

interface SSERequestData {
    connId: string;
    requestId: string;
    path: string;
    params?: any;
}

class SSERequest {
    constructor(private monitor: ISSERequestsMonitor, private connId: string) {

    }
    get<R>(path: string, params?: any): Promise<R> {
        const requestId = generateSecureToken(10);
        this.monitor.sendMessage(this.connId, 'sse-request', { connId: this.connId, requestId, path, params});
        const tracker = this.monitor.createRequestTracker(this.connId, requestId);
        return new Promise<R>((resolve, reject) => {
            tracker.once('completed', (data: any) => {
                resolve(data as R);
            });
        });
    }
}