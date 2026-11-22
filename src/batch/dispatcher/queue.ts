import { EventEmitter } from 'node:events';
import assert from 'assert';
import { getRandomInt } from '../../utils.js';
import {JobId, TaskIndex, TaskItems} from "../types.js";

export interface QueueEvents {
    "tasks-queued": () => void;
    "job-queued": (jobId: JobId, numTasks: number) => void;
    "job-dequeued": (jobId: JobId) => void;
    "queue-empty": () => void;
    "opened": () => void;
    "closed": () => void;
    "change": () => void;
}

export interface QueueJSON {
    closed: boolean;
    numJobs: number;
    numTasks: number;
    jobs: { jobId: JobId; numTasks: number }[];
}

type TaskList = TaskIndex[];

export class Queue extends EventEmitter {
    private _closed: boolean = false;
    private _queue: Map<JobId, TaskList> = new Map<JobId, TaskList>();
    private _jobsList: JobId[] = [];
    constructor() {
        super();
    }
    on<K extends keyof QueueEvents>(
        event: K,
        listener: QueueEvents[K]
    ): this {
        return super.on(event, listener);
    }
    off<K extends keyof QueueEvents>(
        event: K,
        listener: QueueEvents[K]
    ): this {
        return super.off(event, listener);
    }
    emit<K extends keyof QueueEvents>(
        event: K,
        ...args: Parameters<QueueEvents[K]>
    ): boolean {
        return super.emit(event, ...args);
    }
    once<K extends keyof QueueEvents>(
        event: K,
        listener: QueueEvents[K]
    ): this {
        return super.once(event, listener);
    }
    safeEmit<K extends keyof QueueEvents>(
        event: K,
        ...args: Parameters<QueueEvents[K]>
    ): void {
        try {
            super.emit(event, ...args);
        } catch (err) {
            console.error(`Queue "${event}" listener threw:`, err);
        }
    }
    hasJob(jobId: JobId): boolean {
        return this._queue.has(jobId);
    }
    get numJobs(): number {
        return this._jobsList.length;
    }
    get isEmpty(): boolean {
        return this._jobsList.length === 0;
    }
    get numTasks(): number {
        let total:number = 0;
        for (const tasks of this._queue.values()) {
            total += tasks.length;
        }
        return total;
    }
    get closed(): boolean {
        return this._closed;
    }
    set closed(value: boolean) {
        if (this._closed !== value) {
            this._closed = value;
            if (this._closed) {
                this.safeEmit("closed");
            } else {
                this.safeEmit("opened");
            }
            this.safeEmit("change");
        }
    }
    queueJob(jobId: JobId, numTasks: number): void {
        if (this._closed) {
            throw new Error('Cannot queue job: Queue is closed.');
        }
        assert(!this._queue.has(jobId), `Job ID ${jobId} already exists in the queue.`);
        assert(numTasks > 0, `Number of tasks must be greater than zero for job ${jobId}.`);
        this._queue.set(jobId, Array.from({ length: numTasks }, (_, i) => i));
        this._jobsList.push(jobId);
        this.safeEmit("job-queued", jobId, numTasks);
        this.safeEmit("tasks-queued");
        this.safeEmit("change");
    }
    // call this when you are killing a job
    removeJob(jobId: JobId): void {
        if (this._queue.delete(jobId)) {
            const index = this._jobsList.indexOf(jobId);
            assert(index !== -1, `Job ID ${jobId} not found in jobs list.`);
            this._jobsList.splice(index, 1);
            if (this.isEmpty) {
                this.safeEmit("queue-empty");
            }
            this.safeEmit("change");
        }
    }
    dequeueTasks(maxTasksToDequeue: number): TaskItems {
        const ret: TaskItems = [];
        for (let i = 0; i < maxTasksToDequeue; i++) {
            if (this.isEmpty) {
                break;
            }
            const n = this.numJobs;
            assert(n > 0, 'No jobs available to dequeue.');
            const jobIndexPicked = getRandomInt(n - 1);
            assert(jobIndexPicked >= 0 && jobIndexPicked < this._jobsList.length, `Picked job index ${jobIndexPicked} is out of bounds.`);
            const jobIdPicked = this._jobsList[jobIndexPicked];
            const jobTasks = this._queue.get(jobIdPicked);
            assert(jobTasks !== undefined && jobTasks.length > 0, `No tasks available for job Id ${jobIdPicked}.`);
            const taskIndex = jobTasks.shift();
            ret.push({ j: jobIdPicked, t: taskIndex! });
            // If no more tasks left for this job, remove it from the queue
            if (jobTasks && jobTasks.length === 0) {
                this._queue.delete(jobIdPicked);
                this._jobsList.splice(jobIndexPicked, 1);
                this.safeEmit("job-dequeued", jobIdPicked);
                if (this.isEmpty) {
                    this.safeEmit("queue-empty");
                }
            }
        }
        if (ret.length > 0) {
            this.safeEmit("change");
        }
        return ret;
    }
    toJSON(): QueueJSON {
        const jobsArray: { jobId: JobId; numTasks: number }[] = [];
        for (const [jobId, tasks] of this._queue.entries()) {
            jobsArray.push({ jobId, numTasks: tasks.length });
        }
        return {
            closed: this._closed,
            numJobs: this.numJobs,
            numTasks: this.numTasks,
            jobs: jobsArray
        };
    }
}