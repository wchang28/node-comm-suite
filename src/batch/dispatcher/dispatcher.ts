import { EventEmitter } from 'node:events';
import { ConnectionManager } from '../../sse-request/connection-manager.js';
import { Queue, QueueJSON } from './queue.js';
import { Node, NodesTracker } from './nodes.js'
import { DispatchSession, DispatchResultRow } from './dispatch-session.js';
import { JobId, NodeIdentifyInfo, NodeInfoRow, NodeId, TaskItem } from '../types.js';
import { NodesJobKiller, JobKillStatRow } from './nodes-job-killer.js';

export interface DispatcherEvents {
    "enabled": () => void;
    "disabled": () => void;
    "dispatching": (sessionId: string) => void;
    "dispatch-completed": (dispatchSession: DispatchSession) => void;
    "node-to-be-dispatched-one-task": (nodeId: NodeId, taskItem: TaskItem) => void;
    "one-task-dispatched": (resultRow: DispatchResultRow) => void;
}

export interface DispatcherJSON {
    enabled: boolean;
    queue: QueueJSON;
    nodes: NodeInfoRow[];
    dispatching: boolean;
    jobsKillStats: JobKillStatRow[];
}

export class Dispatcher extends EventEmitter {
    private _enabled: boolean = true;
    public queue: Queue = new Queue();
    public nodesTracker: NodesTracker = new NodesTracker();
    private _currentDispatchSession: DispatchSession | null = null;
    public nodesJobKiller: NodesJobKiller;
    constructor(private _connectionManager: ConnectionManager, jobKillRetries: number = 2, jobKillTryIntervalMS: number = 2000) {
        super();
        this.nodesJobKiller = new NodesJobKiller(this.nodesTracker, jobKillRetries, jobKillTryIntervalMS);
        this._connectionManager.on("connected", async (connection) => {
            const nodeId = connection.connId;
            const result = await connection.request<NodeIdentifyInfo>("/identify", "GET");
            const {sessionId, hostname, availableCPUs, currentLoading} = result;
            const node = new Node(connection, nodeId, sessionId, hostname, availableCPUs, currentLoading);
            this.nodesTracker.addNode(node);
        }).on("disconnected", (connection) => {
            const nodeId = connection.connId;
            this.nodesTracker.removeNode(nodeId);
        });
        this.queue.on("tasks-queued", () => {
            this.dispatchTasksIfNecessary();
        });
        this.nodesTracker.on("usable-cpus-increased", () => {
            this.dispatchTasksIfNecessary();
        });
        this.on("enabled", () => {
            this.dispatchTasksIfNecessary();
        }).on("dispatch-completed", () => {
            this.dispatchTasksIfNecessary();
        });
    }
    get enabled(): boolean {
        return this._enabled;
    }
    set enabled(value: boolean) {
        if (this._enabled !== value) {
            this._enabled = value;
            if (this._enabled) {
                this.safeEmit('enabled');
            } else {
                this.safeEmit('disabled');
            }
        }
    }
    get dispatching(): boolean {
        return (this._currentDispatchSession !== null);
    }
    get idle(): boolean {
        return (this._currentDispatchSession === null);
    }
    // await for the dispatcher becomes idle (no current dispatch session)
    async forIdle() {
        let idlePromise: Promise<void> = Promise.resolve();
        if (this._currentDispatchSession !== null) {    // currenly dispatching tasks
            const dispatchSession = this._currentDispatchSession;
            idlePromise = new Promise<void>((resolve) => {
                dispatchSession.once("session-completed", () => {
                    resolve();
                });
            });
        }
        await idlePromise;
    }
    private set currentDispatchingSession(session: DispatchSession | null) {
        if (this._currentDispatchSession == null && session !== null) { // Idle -> Dispatching
            this._currentDispatchSession = session;
            this.safeEmit('dispatching', session.sessionId);
        } else if (this._currentDispatchSession !== null && session === null) { // Dispatching -> Idle
            const oldSession = this._currentDispatchSession;
            this._currentDispatchSession = null;
            this.safeEmit('dispatch-completed', oldSession);
        }
    }
    private async dispatchTasksIfNecessary() {
        if (this._enabled && !this.dispatching && !this.queue.isEmpty) {
            const usableCpus = this.nodesTracker.usableCpus;
            if (usableCpus.length > 0) {
                const tasks = this.queue.dequeueTasks(usableCpus.length);
                const dispatchSession = new DispatchSession(this.nodesTracker);
                const nodeToBeDispatchedOneTaskListener = (nodeId: NodeId, taskItem: TaskItem) => {
                    this.safeEmit("node-to-be-dispatched-one-task", nodeId, taskItem);
                };
                const oneTaskDispatchedListener = (row: DispatchResultRow) => {
                    this.safeEmit("one-task-dispatched", row);
                };
                dispatchSession.once("cpus-usage-assignment", (cpusUsage) => {
                    this.nodesTracker.increaseCPUUsageOnNodes(cpusUsage); // increase CPU usage on nodes based on the dispatch assignments by assuming every task dispatch to the node is successful.
                })
                .on("node-to-be-dispatched-one-task", nodeToBeDispatchedOneTaskListener)
                .on("one-task-dispatched", oneTaskDispatchedListener)
                ;
                this.currentDispatchingSession = dispatchSession;   // mark the start of a dispatch session
                const errors = await dispatchSession.dispatch(usableCpus, tasks);
                dispatchSession.off("node-to-be-dispatched-one-task", nodeToBeDispatchedOneTaskListener);
                dispatchSession.off("one-task-dispatched", oneTaskDispatchedListener);
                this.nodesTracker.decreaseCPUUsageOnNodes(errors);  // for nodes where task dispatch failed, decrease/release the CPU usage accordingly
                this.currentDispatchingSession = null;   // mark the end of a dispatch session
            }
        }
    }
    async killJob(jobId: JobId) {
        this.queue.removeJob(jobId);    // remove the job tasks from the dispatch queue
        await this.nodesJobKiller.killJob(jobId);   // kill the job on the nodes
    }
    get killingJob(): boolean {
        return this.nodesJobKiller.killing;
    }
    get killingJobId(): JobId | null {
        return this.nodesJobKiller.killingJobId;
    }
    toJSON(): DispatcherJSON {
        return {
            enabled: this._enabled,
            queue: this.queue.toJSON(),
            nodes: this.nodesTracker.toJSON(),
            dispatching: this.dispatching,
            jobsKillStats: this.nodesJobKiller.toJSON()
        };
    }
    on<K extends keyof DispatcherEvents>(
        event: K,
        listener: DispatcherEvents[K]
    ): this {
        return super.on(event, listener);
    }
    off<K extends keyof DispatcherEvents>(
        event: K, listener: DispatcherEvents[K]
    ): this {
        return super.off(event, listener);
    }
    emit<K extends keyof DispatcherEvents>(
        event: K,
        ...args: Parameters<DispatcherEvents[K]>
    ): boolean {
        return super.emit(event, ...args);
    }
    once<U extends keyof DispatcherEvents>(
        event: U, listener: DispatcherEvents[U]
    ): this {
        return super.once(event, listener);
    }
    private safeEmit<U extends keyof DispatcherEvents>(
        event: U, ...args: Parameters<DispatcherEvents[U]>
    ): void {
        try {
            this.emit(event, ...args);
        } catch (err) {
            console.error(`Dispatcher "${event}" listener threw:`, err);
        }
    }
}