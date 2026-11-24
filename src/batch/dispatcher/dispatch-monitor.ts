import {EventEmitter} from 'node:events';
import {Pooling} from '../../pooling.js';
import { Dispatcher} from './dispatcher.js';
import { NodesTrackerJSON, QueueJSON, JobKillStatRow, DispatcherStates } from '../types.js';

export interface DispatchMonitorEvents {
    "queue-change": (data: QueueJSON) => void;
    "nodes-change": (data: NodesTrackerJSON) => void;
    "job-killer-change": (data: JobKillStatRow[]) => void;
    "dispatcher-change": (data: DispatcherStates) => void;
    "started": () => void;
    "stopping": () => void;
    "stopped": () => void;
}

export interface DirtyFlags {
    queueDirty: boolean;
    nodesDirty: boolean;
    dispatcherDirty: boolean;
}

export class DispatchMonitor extends EventEmitter {
    private pooling_: Pooling<void>;
    private queueDirty: boolean = false;
    private nodesDirty: boolean = false;
    private dispatcherDirty: boolean = false;
    get dirtyFlags(): DirtyFlags {
        return {
            queueDirty: this.queueDirty,
            nodesDirty: this.nodesDirty,
            dispatcherDirty: this.dispatcherDirty
        };
    }
    constructor(dispatcher: Dispatcher, poolingIntervalMS: number = 1000) {
        super();
        dispatcher.on("queue-change", () => {
            if (!this.queueDirty) {
                this.queueDirty = true;
            }
        });
        dispatcher.nodesTracker.on("change", () => {
            if (!this.nodesDirty) {
                this.nodesDirty = true;
            }
        });
        dispatcher.on("job-killer-change", () => {
            this.safeEmit("job-killer-change", dispatcher.jobKillerJSON);
        });
        dispatcher.on("change", () => {
            if (!this.dispatcherDirty) {
                this.dispatcherDirty = true;
            }
        });
        const pooling = new Pooling<void>(async () => {
            if (this.queueDirty) {
                this.queueDirty = false;
                this.safeEmit("queue-change", dispatcher.queueJSON);
            }
            if (this.nodesDirty) {
                this.nodesDirty = false;
                this.safeEmit("nodes-change", dispatcher.nodesTracker.toJSON());
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