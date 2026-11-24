import { EventEmitter } from 'node:events';
import { ConnectionManager } from '../../sse-request/connection-manager.js';
import { Queue} from './queue.js';
import { Node, NodesTracker } from './nodes.js'
import { DispatchSession, DispatchResultRow } from './dispatch-session.js';
import type {
    JobId,
    NodeIdentifyInfo,
    NodeId,
    TaskItem,
    DispatcherState,
    DispatcherStates,
    DispatcherJSON,
    QueueJSON,
    JobKillStatRow
} from '../types.js';
import { NodesJobKiller } from './nodes-job-killer.js';
import {Pooling} from "../../pooling.js";

export interface DispatcherEvents {
    "change": () => void;
    "state-change": (oldState: DispatcherState, newState: DispatcherState) => void;
    "queue-change": () => void;
    "job-killer-change": () => void;
    "killing-job": (jobId: JobId) => void;
    "job-killed": (jobId: JobId) => void;
    "job-kill-retry": (jobId: JobId, retryIndex: number, numProcessKilled: number) => void;
    "job-kill-error": (jobId: JobId, error: unknown) => void;
    "enabled": () => void;
    "disabled": () => void;
    "dispatching": (sessionId: string) => void;
    "dispatch-completed": (dispatchSession: DispatchSession) => void;
    "node-to-be-dispatched-one-task": (nodeId: NodeId, taskItem: TaskItem) => void;
    "one-task-dispatched": (resultRow: DispatchResultRow) => void;
    "started": () => void;
    "stopping": () => void;
    "stopped": () => void;
}

export class Dispatcher extends EventEmitter {
    private _state: DispatcherState = "stopped";
    private _enabled: boolean = true;
    private _queue: Queue = new Queue();
    public nodesTracker: NodesTracker = new NodesTracker();
    private _currentDispatchSession: DispatchSession | null = null;
    private _nodesJobKiller: NodesJobKiller;
    private _nodesSystemLoadPooling: Pooling<void>;
    constructor(connectionManager: ConnectionManager, jobKillRetries: number = 2, jobKillTryIntervalMS: number = 2000) {
        super();
        connectionManager.on("connected", async (connection) => {
            try {
                const nodeId = connection.connId;
                const result = await connection.request<NodeIdentifyInfo>("/identify", "GET");  // ask the node to identify itself
                const {sessionId, hostname, totalTaskSlots, currentLoading} = result;
                const node = new Node(connection, nodeId, sessionId, hostname, totalTaskSlots, currentLoading);
                this.nodesTracker.addNode(node);
            } catch (err) {}
        }).on("disconnected", (connection) => {
            const nodeId = connection.connId;
            this.nodesTracker.removeNode(nodeId);
        });
        this._queue.on("tasks-queued", () => {
            this.dispatchTasksIfNecessary();
        }).on("change", () => {
            this.safeEmit('queue-change');
        });
        this.nodesTracker.on("task-slots-change", () => {
            this.dispatchTasksIfNecessary();
        });
        this.on("enabled", () => {
            this.dispatchTasksIfNecessary();
        }).on("dispatch-completed", () => {
            this.dispatchTasksIfNecessary();
        });
        const jobKiller = new NodesJobKiller(this.nodesTracker, jobKillRetries, jobKillTryIntervalMS);
        jobKiller.on("change", () => {
            this.safeEmit("job-killer-change");
        }).on("killing", (jobId: JobId) => {
            this.safeEmit("killing-job", jobId);
        }).on("killed", (jobId: JobId) => {
            this.safeEmit("job-killed", jobId);
        }).on("kill-retry", (jobId: JobId, retryIndex: number, numProcessKilled: number) => {
            this.safeEmit("job-kill-retry", jobId, retryIndex, numProcessKilled);
        }).on("kill-error", (jobId: JobId, error: unknown) => {
            this.safeEmit("job-kill-error", jobId, error);
        });
        this._nodesJobKiller = jobKiller;
        this._nodesSystemLoadPooling = new Pooling<void>(async () => {
            await this.nodesTracker.updateNodesSystemLoading();
        }, 30000);  // update system loading every 30 seconds
    }
    get state(): DispatcherState {
        return this._state;
    }
    private set state(newState: DispatcherState) {
        const oldState = this._state;
        if (oldState !== newState) {
            this._state = newState;
            if (newState == "started") {
                this.safeEmit('started');
            } else if (newState == "stopping") {
                this.safeEmit('stopping');
            } else if (newState == "stopped") {
                this.safeEmit('stopped');
            }
            this.safeEmit('state-change', oldState, newState);
            this.safeEmit('change');
        }
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
            this.safeEmit('change');
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
            this.safeEmit('change');
        } else if (this._currentDispatchSession !== null && session === null) { // Dispatching -> Idle
            const oldSession = this._currentDispatchSession;
            this._currentDispatchSession = null;
            this.safeEmit('dispatch-completed', oldSession);
            this.safeEmit('change');
        }
    }
    private async dispatchTasksIfNecessary() {
        if (this._enabled && !this.dispatching && !this._queue.isEmpty) {
            const usableTaskSlots = this.nodesTracker.usableTaskSlots;
            if (usableTaskSlots.length > 0) {
                const tasks = this._queue.dequeueTasks(usableTaskSlots.length);
                const dispatchSession = new DispatchSession(this.nodesTracker);
                this.currentDispatchingSession = dispatchSession;   // mark the start of a dispatch session
                const nodeToBeDispatchedOneTaskListener = (nodeId: NodeId, taskItem: TaskItem) => {
                    this.safeEmit("node-to-be-dispatched-one-task", nodeId, taskItem);
                };
                const oneTaskDispatchedListener = (row: DispatchResultRow) => {
                    this.safeEmit("one-task-dispatched", row);
                };
                dispatchSession.once("task-slots-usage-assignment", (taskSlotsUsage) => {
                    this.nodesTracker.increaseTaskSlotsUsageOnNodes(taskSlotsUsage); // increase task slots usage on nodes based on the dispatch assignments by assuming every task dispatch to the node is successful.
                })
                .on("node-to-be-dispatched-one-task", nodeToBeDispatchedOneTaskListener)
                .on("one-task-dispatched", oneTaskDispatchedListener)
                ;
                const errors = await dispatchSession.dispatch(usableTaskSlots, tasks);
                dispatchSession.off("node-to-be-dispatched-one-task", nodeToBeDispatchedOneTaskListener);
                dispatchSession.off("one-task-dispatched", oneTaskDispatchedListener);
                this.nodesTracker.decreaseTaskSlotsUsageOnNodes(errors);  // for nodes where task dispatch failed, decrease/release the task slots usage accordingly
                this.currentDispatchingSession = null;   // mark the end of a dispatch session
            }
        }
    }
    closeQueue() {
        this._queue.closed = true;
    }
    openQueue() {
        this._queue.closed = false;
    }
    get queueClosed(): boolean {
        return this._queue.closed;
    }
    queueJob(jobId: JobId, numTasks: number) {
        if (this.state == "started") {
            this._queue.queueJob(jobId, numTasks);
        } else {
            throw new Error(`Cannot queue job "${jobId}" because dispatcher is not started.`);
        }
    }
    async killJob(jobId: JobId) {
        if (this.state == "started") {
            this._queue.removeJob(jobId);    // remove the job tasks from the dispatch queue
            await this._nodesJobKiller.killJob(jobId);   // kill the job on the nodes
        } else {
            throw new Error(`Cannot kill job "${jobId}" because dispatcher is not started.`);
        }
    }
    get isKillingJob(): boolean {
        return this._nodesJobKiller.killing;
    }
    get killingJobId(): JobId | null {
        return this._nodesJobKiller.killingJobId;
    }
    start() {
        if (this.state == "stopped") {
            this._nodesSystemLoadPooling.start();
            this._nodesJobKiller.start();
            this.state = "started";
        }
    }
    async stop() {
        if (this.state == "started") {
            this.state = "stopping";
            this._queue.clear();
            await this.forIdle();
            await this._nodesSystemLoadPooling.stop();
            await this._nodesJobKiller.stop();
            this.state = "stopped";
        }
    }
    get states(): DispatcherStates {
        return {
            state: this._state,
            enabled: this._enabled,
            dispatching: this.dispatching
        };
    }
    get queueJSON(): QueueJSON {
        return this._queue.toJSON();
    }
    get jobKillerJSON(): JobKillStatRow[] {
        return this._nodesJobKiller.toJSON();
    }
    toJSON(): DispatcherJSON {
        return {
            states: this.states,
            queue: this._queue.toJSON(),
            nodes: this.nodesTracker.toJSON(),
            jobsKillStats: this._nodesJobKiller.toJSON()
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