import { EventEmitter } from 'events';
import { JobId } from "../types.js";

export interface JobsKillQueueEvents {
    "change": [];
    "enqueue": [JobId];
    "dequeue": [JobId];
}

export class JobsKillQueue extends EventEmitter {
    private _queue: JobId[] = [];
    constructor() {
        super();
    }
    enqueue(jobId: JobId) {
        this._queue.push(jobId);
        this.emit('enqueue', jobId);
        this.emit('change');
    }
    dequeue(): JobId | undefined {
        const jobId = this._queue.shift();
        if (jobId !== undefined) {
            this.emit('dequeue', jobId);
            this.emit('change');
        }
        return jobId;
    }
    get numJobs(): number {
        return this._queue.length;
    }
    get isEmpty(): boolean {
        return this._queue.length === 0;
    }
    toJSON(): JobId[] {
        return [...this._queue];
    }
    on<K extends keyof JobsKillQueueEvents>(
        event: K,
        listener: (...args: JobsKillQueueEvents[K]) => void
    ): this {
        return super.on(event, listener);
    }
    emit<K extends keyof JobsKillQueueEvents>(
        event: K,
        ...args: JobsKillQueueEvents[K]
    ): boolean {
        return super.emit(event, ...args);
    }
}