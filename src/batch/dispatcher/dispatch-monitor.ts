import {EventEmitter} from 'node:events';
import {Pooling} from '../../pooling.js';
import { Dispatcher, DispatcherStates } from './dispatcher.js';
import { QueueJSON } from './queue.js';
import { NodeInfoRow } from '../types.js';
import { JobKillStatRow } from './nodes-job-killer.js';

export interface DispatchMonitorEvents {
    "queue-change": (data: QueueJSON) => void;
    "nodes-change": (data: NodeInfoRow[]) => void;
    "job-killer-change": (data: JobKillStatRow[]) => void;
    "dispatcher-change": (data: DispatcherStates) => void;
    "started": () => void;
    "stopping": () => void;
    "stopped": () => void;
}

export interface DirtyFlags {
    queueDirty: boolean;
    nodesDirty: boolean;
    jobKillerDirty: boolean;
    dispatcherDirty: boolean;
}

export class DispatchMonitor extends EventEmitter {
    private pooling_: Pooling<void>;
    private queueDirty: boolean = false;
    private nodesDirty: boolean = false;
    private jobKillerDirty: boolean = false;
    private dispatcherDirty: boolean = false;
    get dirtyFlags(): DirtyFlags {
        return {
            queueDirty: this.queueDirty,
            nodesDirty: this.nodesDirty,
            jobKillerDirty: this.jobKillerDirty,
            dispatcherDirty: this.dispatcherDirty
        };
    }
    constructor(dispatcher: Dispatcher, poolingIntervalMS: number = 1000) {
        super();
        dispatcher.queue.on("change", () => {
            if (!this.queueDirty) {
                this.queueDirty = true;
            }
        });
        dispatcher.nodesTracker.on("change", () => {
            if (!this.nodesDirty) {
                this.nodesDirty = true;
            }
        });
        dispatcher.nodesJobKiller.on("change", () => {
            if (!this.jobKillerDirty) {
                this.jobKillerDirty = true;
            }
        });
        dispatcher.on("change", () => {
            if (!this.dispatcherDirty) {
                this.dispatcherDirty = true;
            }
        });
        const pooling = new Pooling<void>(async () => {
            if (this.queueDirty) {
                this.queueDirty = false;
                this.safeEmit("queue-change", dispatcher.queue.toJSON());
            }
            if (this.nodesDirty) {
                this.nodesDirty = false;
                this.safeEmit("nodes-change", dispatcher.nodesTracker.toJSON());
            }
            if (this.jobKillerDirty) {
                this.jobKillerDirty = false;
                this.safeEmit("job-killer-change", dispatcher.nodesJobKiller.toJSON());
            }
            if (this.dispatcherDirty) {
                this.dispatcherDirty = false;
                this.safeEmit("dispatcher-change", dispatcher.states);
            }
        }, poolingIntervalMS); // Example pooling interval of 500ms
        pooling.on("started", () => {
            this.safeEmit("started");
        }).on("stopping", () => {
            this.safeEmit("stopping");
        }).on("stopped", () => {
            this.safeEmit("stopped");
        });
        this.pooling_ = pooling;
    }
    on<U extends keyof DispatchMonitorEvents>(
        event: U, listener: DispatchMonitorEvents[U]
    ): this {
        return super.on(event, listener);
    }
    off<U extends keyof DispatchMonitorEvents>(
        event: U, listener: DispatchMonitorEvents[U]
    ): this {
        return super.off(event, listener);
    }
    emit<U extends keyof DispatchMonitorEvents>(
        event: U, ...args: Parameters<DispatchMonitorEvents[U]>
    ): boolean {
        return super.emit(event, ...args);
    }
    once<U extends keyof DispatchMonitorEvents>(
        event: U, listener: DispatchMonitorEvents[U]
    ): this {
        return super.once(event, listener);
    }
    private safeEmit<U extends keyof DispatchMonitorEvents>(
        event: U, ...args: Parameters<DispatchMonitorEvents[U]>
    ): void {
        try {
            this.emit(event, ...args);
        } catch (err) {
            console.error(`DispatchMonitor "${event}" listener threw:`, err);
        }
    }
    start() {
        this.pooling_.start();
    }
    async stop() {
        await this.pooling_.stop();
    }
}