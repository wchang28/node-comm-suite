import {
    ISystemLoadingLogger,
    NodeSessionId,
    NodeName,
    SystemLoadingInfo,
} from "./types.js";

export class RequestBasedSystemLoadingLogger implements ISystemLoadingLogger {
    constructor(private _url: string) {
    }
    async log(sessionId: NodeSessionId, hostname: NodeName, currentLoading: SystemLoadingInfo) {
        // TODO: Implement the logic to send the system loading info to the server using this._url
        await Promise.resolve();
    }
    async get(sessionIds: NodeSessionId[]) {
        // TODO: Implement the logic to fetch the system loading info for the given session IDs from the server using this._url
        const ret = await Promise.resolve<{sessionId: NodeSessionId, currentLoading: SystemLoadingInfo}[]>([]);
        return ret;
    }
}