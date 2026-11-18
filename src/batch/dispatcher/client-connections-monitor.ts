import { EventEmitter } from "events";
import {ConnectionManager} from "../../sse-request/connection-manager.js";
import type {ConnectionId} from "../../sse-request/types.js";
import {RequestGetSet}  from "./request-get-set.js";

export type MonitoringTopic = "dispatcher" | "recent-jobs" | "connections";

export interface ClientConnectionItem {
    topic: MonitoringTopic;
    connId: ConnectionId;
    remoteAddress: string;
    userId: string;
    userName: string;
}

export interface ClientConnectionsMonitorEvents {
    "change": [];
}

export class ClientConnectionsMonitor extends EventEmitter {
    constructor(
        private dispatcherMonitorConnectionManager: ConnectionManager,
        private recentJobsMonitorConnectionManager: ConnectionManager,
        private connectionsMonitorConnectionManager: ConnectionManager
    ) {
        super();
        dispatcherMonitorConnectionManager.on("change", () => this.emit("change"));
        recentJobsMonitorConnectionManager.on("change", () => this.emit("change"));
        connectionsMonitorConnectionManager.on("change", () => this.emit("change"));
    }
    on<K extends keyof ClientConnectionsMonitorEvents>(
        event: K,
        listener: (...args: ClientConnectionsMonitorEvents[K]) => void
    ): this {
        return super.on(event, listener);
    }
    emit<K extends keyof ClientConnectionsMonitorEvents>(
        event: K,
        ...args: ClientConnectionsMonitorEvents[K]
    ): boolean {
        return super.emit(event, ...args);
    }
    private getMonitorConnectionItems(
        topic: MonitoringTopic,
        connectionManager: ConnectionManager
    ): ClientConnectionItem[] {
        return connectionManager.connectionLists.map(conn => {
            const req_gs = new RequestGetSet(conn.req);
            const user = req_gs.user;
            return {
                topic,
                connId: conn.connId,
                remoteAddress: conn.remoteSocketAddress,
                userId: user?.id ?? "",
                userName: user?.name ?? ""
            };
        });
    }
    toJSON(): ClientConnectionItem[] {
        return [
            ...this.getMonitorConnectionItems("dispatcher", this.dispatcherMonitorConnectionManager),
            ...this.getMonitorConnectionItems("recent-jobs", this.recentJobsMonitorConnectionManager),
            ...this.getMonitorConnectionItems("connections", this.connectionsMonitorConnectionManager)
        ];
    }
}