import { EventEmitter } from 'events';
import { ConnectionManager } from '../sse-request/connection-manager.js';
import { Queue, QueueJSON } from './queue.js';
import { Node, NodesTracker } from './nodes.js'
import { DispatchSession, DispatchResultRow, DispatchSessionJSON } from './dispatch-session.js';
import { JobId, NodeIdentifyInfo, NodeInfoRow } from '../type.js';
import { sleep } from "../utils.js";
import { JobsKillQueue } from './jobs-kill-queue.js';
import assert from "assert";

export interface DispatcherEvents {
    "change": [];
    "enabled": [];
    "disabled": [];
    "dispatching": [sessionId: string];
    "dispatch-completed": [dispatchSession: DispatchSession];
    "one-task-dispatched": [DispatchResultRow];
    "killing-job": [j: JobId];
    "job-kill-retry": [j: JobId, retryIndex: number, numProcessKilled: number];
    "job-kill-completed": [j: JobId];
}

export type JobKillState = "queued" | "killing";

export interface JobKillStatRow {
    j: JobId;
    state: JobKillState;
}

export interface DispatcherJSON {
    enabled: boolean;
    queue: QueueJSON;
    nodes: NodeInfoRow[];
    dispatching: boolean;
    dispatchSession: DispatchSessionJSON | null;
    killingJob: boolean;
    jobsKillStats: JobKillStatRow[];
}

export class Dispatcher extends EventEmitter {
    private _enabled: boolean = true;
    public queue: Queue = new Queue();
    public nodesTracker: NodesTracker = new NodesTracker();
    private _currentDispatchSession: DispatchSession | null = null;
    private _dispatchIdlePromise: Promise<void> = Promise.resolve();
    private _jobsKillQueue: JobsKillQueue = new JobsKillQueue();
    private _killingJobId: JobId | null = null;
    constructor(private _connectionManager: ConnectionManager, public jobKillRetries: number = 2, public jobKillTryIntervalMS: number = 2000) {
        super();
        assert(this.jobKillRetries > 0, "jobKillRetries must be greater than 0");
        assert(this.jobKillTryIntervalMS > 200, "jobKillTryIntervalMS must be greater than 200");
        const me = this;
        this._connectionManager.on("connected", async (connection) => {
            const nodeId = connection.connId;
            const result = await connection.request<NodeIdentifyInfo>("/identify", "GET");
            const {sessionId, hostname, availableCPUs, currentLoading} = result;
            const node = new Node(connection, nodeId, sessionId, hostname, availableCPUs, currentLoading);
            me.nodesTracker.addNode(node);
        }).on("disconnected", (connection) => {
            const nodeId = connection.connId;
            me.nodesTracker.removeNode(nodeId);
        });
        this.queue.on('change', () => {
            me.emit('change');
        }).on("tasks-queued", () => {
            me.dispatchTasksIfNecessary();
        });
        this.nodesTracker.on('change', () => {
            me.emit('change');
        }).on("usable-cpus-increased", () => {
            me.dispatchTasksIfNecessary();
        });
        this.on("enabled", () => {
            me.dispatchTasksIfNecessary();
        }).on("dispatch-completed", () => {
            me.dispatchTasksIfNecessary();
        }).on("job-kill-completed", () => {
            me.killJobIfNecessary();
        });
        this._jobsKillQueue.on('change', () => {
            me.emit('change');
        }).on("enqueue", () => {
            me.killJobIfNecessary();
        });
    }
    get enabled(): boolean {
        return this._enabled;
    }
    set enabled(value: boolean) {
        if (this._enabled !== value) {
            this._enabled = value;
            if (this._enabled) {
                this.emit('enabled');
            } else {
                this.emit('disabled');
            }
            this.emit('change');
        }
    }
    get notDispatching(): Promise<void> {
        return this._dispatchIdlePromise;
    }
    get dispatching(): boolean {
        return (this._currentDispatchSession !== null);
    }
    private set currentDispatchingSession(session: DispatchSession | null) {
        if (this._currentDispatchSession == null && session !== null) {
            this._currentDispatchSession = session;
            this.emit('dispatching', session.sessionId);
            this.emit("change");
        } else if (this._currentDispatchSession !== null && session === null) {
            const oldSession = this._currentDispatchSession;
            this._currentDispatchSession = null;
            this.emit('dispatch-completed', oldSession);
            this.emit("change");
        }
    }
    private async dispatchTasksIfNecessary() {
        if (this._enabled && !this.dispatching && !this.queue.isEmpty) {
            const usableCpus = this.nodesTracker.usableCpus;
            if (usableCpus.length > 0) {
                const tasks = this.queue.dequeueTasks(usableCpus.length);
                const dispatchSession = new DispatchSession(this.nodesTracker);
                const me = this;
                dispatchSession.on("change", () => {
                    me.emit("change");
                }).on("cpus-usage-assignment", (cpusUsage) => {
                    me.nodesTracker.increaseCPUUsageOnNodes(cpusUsage); // increase CPU usage on nodes based on the dispatch assignments by assuming every task dispatch to the node is successful.
                }).on("one-task-dispatched", (row) => {
                    me.emit("one-task-dispatched", row);
                });
                this._dispatchIdlePromise = new Promise<void>((resolve) => {
                    dispatchSession.on("session-completed", () => {
                        resolve();
                    });
                });
                this.currentDispatchingSession = dispatchSession;   // mark the start of a dispatch session
                const errors = await dispatchSession.dispatch(usableCpus, tasks);
                dispatchSession.removeAllListeners();
                this.nodesTracker.decreaseCPUUsageOnNodes(errors);  // for nodes where task dispatch failed, decrease/release the CPU usage accordingly
                this.currentDispatchingSession = null;   // mark the end of a dispatch session
            }
        }
    }
    get killingJob(): boolean {
        return (this._killingJobId !== null);
    }
    get killingJobId(): JobId | null {
        return this._killingJobId;
    }
    set killingJobId(jobId: JobId | null) {
        if (this._killingJobId == null && jobId !== null) { // starting to kill a new job
            this._killingJobId = jobId;
            this.emit("killing-job", jobId);
            this.emit("change");
        } else if (this._killingJobId !== null && jobId === null) { // finished killing the current job
            const killedJobId = this._killingJobId;
            this._killingJobId = null;
            this.emit("job-kill-completed", killedJobId);
            this.emit("change");
        }
    }
    private async killJobIfNecessary() {
        if (!this.killingJob && !this._jobsKillQueue.isEmpty) {
            const jobId = this._jobsKillQueue.dequeue();
            if (jobId !== undefined) {
                this.killingJobId = jobId;
                await this.tryKillJobProcessOnNodes(jobId);
                this.killingJobId = null;
            }
        }
    }
    private async tryKillJobProcessOnNodes(jobId: JobId) {
        let retries = 0;
        while (true) {
            await this.notDispatching;
            const n = await this.nodesTracker.killJobProcesses(jobId);
            this.emit("job-kill-retry", jobId, retries, n);
            retries++;
            if (retries < this.jobKillRetries) {
                await sleep(this.jobKillTryIntervalMS);
            } else {
                break;
            }
        }
    }
    killJob(jobId: JobId): Promise<void> {
        this.queue.removeJob(jobId);    // remove the job tasks from the dispatch queue
        this._jobsKillQueue.enqueue(jobId); // inject the job into the kill queue
        const me = this;
        const killCompletion = new Promise<void>((resolve) => {
            const listener = (completedJobId: JobId) => {
                if (completedJobId == jobId) {
                    me.off("job-kill-completed", listener);
                    resolve();
                }
            };
            me.on("job-kill-completed", listener);
        });
        return killCompletion;
    }
    get jobsKillStats(): JobKillStatRow[] {
        const ret:JobKillStatRow[] = [];
        const killingJobId = this.killingJobId;
        if (killingJobId !== null) {
            ret.push({
                j: killingJobId,
                state: "killing"
            });
        }
        const queuedJobIds = this._jobsKillQueue.toJSON();
        const queuedRows = queuedJobIds.map((jobId) => {
            const row: JobKillStatRow = {
                j: jobId,
                state: "queued"
            };
            return row;
        });
        ret.push(...queuedRows);
        return ret;
    }
    toJSON(): DispatcherJSON {
        return {
            enabled: this._enabled,
            queue: this.queue.toJSON(),
            nodes: this.nodesTracker.toJSON(),
            dispatching: this.dispatching,
            dispatchSession: (this._currentDispatchSession ? this._currentDispatchSession.toJSON() : null),
            killingJob: this.killingJob,
            jobsKillStats: this.jobsKillStats
        };
    }
    on<K extends keyof DispatcherEvents>(
        event: K,
        listener: (...args: DispatcherEvents[K]) => void
    ): this {
        return super.on(event, listener);
    }
    emit<K extends keyof DispatcherEvents>(
        event: K,
        ...args: DispatcherEvents[K]
    ): boolean {
        return super.emit(event, ...args);
    }
}