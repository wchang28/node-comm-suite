import { EventEmitter } from "node:events";
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
    "change": () => void;
}

// class that monitors client connections for different topics and emits change events when the connection lists change
export class ClientConnectionsMonitor extends EventEmitter {
    constructor(
        private dispatcherEventsConnector: ConnectionManager,
        private recentJobsEventsConnector: ConnectionManager,
        private connectionsEventsConnector: ConnectionManager
    ) {
        super();
        dispatcherEventsConnector.on("change", () => this.safeEmit("change"));
        recentJobsEventsConnector.on("change", () => this.safeEmit("change"));
        connectionsEventsConnector.on("change", () => this.safeEmit("change"));
    }
    on<K extends keyof ClientConnectionsMonitorEvents>(
        event: K,
        listener: ClientConnectionsMonitorEvents[K]
    ): this {
        return super.on(event, listener);
    }
    off<K extends keyof ClientConnectionsMonitorEvents>(
        event: K,
        listener: ClientConnectionsMonitorEvents[K]
    ): this {
        return super.off(event, listener);
    }
    emit<K extends keyof ClientConnectionsMonitorEvents>(
        event: K,
        ...args: Parameters<ClientConnectionsMonitorEvents[K]>
    ): boolean {
        return super.emit(event, ...args);
    }
    once<K extends keyof ClientConnectionsMonitorEvents>(
        event: K,
        listener: ClientConnectionsMonitorEvents[K]
    ): this {
        return super.once(event, listener);
    }
    private safeEmit<K extends keyof ClientConnectionsMonitorEvents>(
        event: K, ...args: Parameters<ClientConnectionsMonitorEvents[K]>
    ): void {
        try {
            this.emit(event, ...args);
        } catch (err) {
            console.error(`ClientConnectionsMonitor "${event}" listener threw:`, err);
        }
    }
    private getMonitorConnectionItems(
        topic: MonitoringTopic,
        connectionManager: ConnectionManager
    ): ClientConnectionItem[] {
        return connectionManager.connections.map(connection => {
            const req_gs = new RequestGetSet(connection.req);
            const user = req_gs.user;
            return {
                topic,
                connId: connection.connId,
                remoteAddress: connection.remoteSocketAddress,
                userId: user?.id ?? "",
                userName: user?.name ?? ""
            };
        });
    }
    toJSON(): ClientConnectionItem[] {
        return [
            ...this.getMonitorConnectionItems("dispatcher", this.dispatcherEventsConnector),
            ...this.getMonitorConnectionItems("recent-jobs", this.recentJobsEventsConnector),
            ...this.getMonitorConnectionItems("connections", this.connectionsEventsConnector)
        ];
    }
}