import { EventEmitter } from "node:events";

export type Processor<I, O> = (input: I) => Promise<O>;

interface Job<I, O> {
    input: I;
    resolve: (o: O) => void;
    reject: (e: unknown) => void;
}

interface QueueEvents<I, O> {
    "enqueue": (job: Job<I, O>) => void;
    "dequeue": (job: Job<I, O>) => void;
    "empty": () => void;
    "change": () => void;
}

class Queue<I, O> extends EventEmitter {
    private jobs_: Job<I, O>[] = [];
    constructor() {
        super();
    }
    on<K extends keyof QueueEvents<I, O>>(event: K, listener: QueueEvents<I, O>[K]): this {
        return super.on(event, listener);
    }
    off<K extends keyof QueueEvents<I, O>>(event: K, listener: QueueEvents<I, O>[K]): this {
        return super.off(event, listener);
    }
    emit<K extends keyof QueueEvents<I, O>>(event: K, ...args: Parameters<QueueEvents<I, O>[K]>): boolean {
        return super.emit(event, ...args);
    }
    once<K extends keyof QueueEvents<I, O>>(event: K, listener: QueueEvents<I, O>[K]): this {
        return super.once(event, listener);
    }
    private safeEmit<U extends keyof QueueEvents<I, O>>(
        event: U, ...args: Parameters<QueueEvents<I, O>[U]>
    ): void {
        try {
            this.emit(event, ...args);
        } catch (err) {
            console.error(`Queue "${event}" listener threw:`, err);
        }
    }
    enqueue(job: Job<I, O>): void {
        this.jobs_.push(job);
        this.safeEmit("enqueue", job);
        this.safeEmit("change");
    }
    dequeue(): Job<I, O> | undefined {
        const job = this.jobs_.shift();
        if (job) {
            this.safeEmit("dequeue", job);
            if (this.isEmpty) {
                this.safeEmit("empty");
            }
            this.safeEmit("change");
        }
        return job;
    }
    get length(): number {
        return this.jobs_.length;
    }
    get jobs(): readonly Job<I, O>[] {
        return this.jobs_;
    }
    get isEmpty(): boolean {
        return (this.jobs_.length === 0);
    }
    // Clears the queue and rejects all pending jobs with an "Job aborted" error.
    clear(): void {
        const n = this.jobs_.length;
        for (const job of this.jobs_) {
            job.reject(new Error("Job aborted"));
        }
        this.jobs_ = [];
        if (n > 0) {
            this.safeEmit("empty");
            this.safeEmit("change");
        }
    }
    async forEmpty() {
        if (!this.isEmpty) {
            await new Promise<void>((resolve) => {
                this.once("empty", () => resolve());
            });
        }
    }
}

export interface SequentialProcessorEvents<I, O> {
    "change": () => void;
    "processing": (input: I) => void;
    "processed": (input: I, output: O) => void; // successfully processed a job
    "process-error": (input: I, error: unknown) => void;    // failed to process a job
    "idle": () => void; // processor became idle
    "started": () => void; // processor started
    "stopped": () => void; // processor stopped
    "stopping": () => void; // processor stopping
    "state-change": (oldState: State, newState: State) => void; // processor state changed
}

export type JobState = "queued" | "processing";

export interface JobSummaryRow<I> {
    state: JobState;
    input: I;
}

export type State = "started" | "stopping" | "stopped";

export interface SequentialProcessorJSON<I> {
    state: State;
    processing: boolean;
    pending: number;
    jobs: JobSummaryRow<I>[];
}

export class SequentialProcessor<I, O> extends EventEmitter {
    private state_: State = "stopped";
    private queue_: Queue<I, O> = new Queue<I, O>();
    private processingJob_: Job<I, O> | null = null;
    constructor(private readonly processor_: Processor<I, O>) {
        super();
        this.queue_.on("enqueue", () => {
            this.safeEmit("change");
            this.drain();
        });
        this.on("idle", () => {
            this.drain();
        });
    }
    on<U extends keyof SequentialProcessorEvents<I, O>>(
        event: U, listener: SequentialProcessorEvents<I, O>[U]
    ): this {
        return super.on(event, listener);
    }
    off<U extends keyof SequentialProcessorEvents<I, O>>(
        event: U, listener: SequentialProcessorEvents<I, O>[U]
    ): this {
        return super.off(event, listener);
    }
    emit<U extends keyof SequentialProcessorEvents<I, O>>(
        event: U, ...args: Parameters<SequentialProcessorEvents<I, O>[U]>
    ): boolean {
        return super.emit(event, ...args);
    }
    once<U extends keyof SequentialProcessorEvents<I, O>>(
        event: U, listener: SequentialProcessorEvents<I, O>[U]
    ): this {
        return super.once(event, listener);
    }
    private safeEmit<U extends keyof SequentialProcessorEvents<I, O>>(
        event: U, ...args: Parameters<SequentialProcessorEvents<I, O>[U]>
    ): void {
        try {
            this.emit(event, ...args);
        } catch (err) {
            console.error(`SequentialProcessor "${event}" listener threw:`, err);
        }
    }
    get state(): State {
        return this.state_;
    }
    private set state(newState: State) {
        const oldState: State = this.state_;
        if (oldState !== newState) {
            this.state_ = newState;
            if (newState === "started") {
                this.safeEmit("started");
            } else if (newState === "stopped") {
                this.safeEmit("stopped");
            } else if (newState === "stopping") {
                this.safeEmit("stopping");
            }
            this.safeEmit("state-change", oldState, newState);
            this.safeEmit("change");
        }
    }
    // returns true if currently processing a job, false if idle
    get processing(): boolean {
        return (this.processingJob_ != null);
    }
    // Returns true if the processor is idle (not currently processing a job).
    get idle(): boolean {
        return (this.processingJob_ == null);
    }
    // The input currently being processed, or null if idle.
    get processingInput(): I | null {
        return (this.processingJob_ ? this.processingJob_.input : null);
    }
    // Sets the currently processing job and emits appropriate events based on the state transition
    private set processingJob(newJob: Job<I, O> | null) {
        const oldJob = this.processingJob_;
        if (oldJob != newJob) {
            this.processingJob_ = newJob;
            this.safeEmit("change");
            const goingFromProcessingToIdle = (oldJob != null && newJob == null);   // Processing -> Idle
            const goingFromIdleToProcessing = (oldJob == null && newJob != null);   // Idle -> Processing
            if (goingFromProcessingToIdle) {    // Processing -> Idle
                this.safeEmit("idle"); // emit idle event when transitioning from processing to idle
            } else if (goingFromIdleToProcessing) { // Idle -> Processing
                this.safeEmit("processing", newJob!.input);
            }
        }
    }
    // Returns the number of jobs currently pending in the queue.
    get pending(): number {
        return this.queue_.length;
    }
    // Submit a job to the queue and return a promise that resolves with the output or rejects with an error.
    submit(input: I): Promise<O> {
        if (this.state == "started") {
            return new Promise<O>((resolve, reject) => {
                this.queue_.enqueue({ input, resolve, reject });
            });
        } else {
            return Promise.reject(new Error("Processor is not started"));
        }
    }
    // Starts processing the next job in the queue (if there is one) if the processor is idle.
    private drain() {
        if (this.idle) {    // if currently idle, start processing the next job in the queue if there is one
            const job = this.queue_.dequeue();
            if (job) {  // if there is a job got dequeued, start processing it
                this.processingJob = job; // Idle -> Processing
                new Promise<O>((resolve) => resolve(this.processor_(job.input)))
                .then((output) => {
                    this.safeEmit("processed", job.input, output);
                    this.processingJob = null; // Processing -> Idle
                    job.resolve(output);
                })
                .catch((err) => {
                    this.safeEmit("process-error", job.input, err);
                    this.processingJob = null; // Processing -> Idle
                    job.reject(err);
                })
            }
        }
    }
    // await for the processor to become idle
    async forIdle() {
        if (!this.idle) {    // currenly processing job
            await new Promise<void>((resolve) => {
                this.once("idle", () => resolve());
            });
        }
    }
    // Returns an array of inputs currently queued for processing.
    get queuedInputs(): I[] {
        return this.queue_.jobs.map(job => job.input);
    }
    // Returns a summary of all jobs, including the currently processing job and queued jobs.
    toJSON(): SequentialProcessorJSON<I>{
        const rows: JobSummaryRow<I>[] = this.queue_.jobs.map(job => ({ input: job.input, state: "queued" }));
        if (this.processingJob_) {  // if there is a job currently being processed, add it to the summary as "processing"
            rows.unshift({ input: this.processingJob_.input, state: "processing" });    // add the currently processing job at the beginning of the summary
        }
        return {
            state: this.state,
            processing: this.processing,
            pending: this.pending,
            jobs: rows
        };
    }
    start() {
        if (this.state == "stopped") {
            this.state = "started";
        }
    }
    async stop() {
        if (this.state === "started") {
            this.state = "stopping";
            this.queue_.clear();    // clear the queue
            await this.queue_.forEmpty();   // wait for the queue to become empty
            await this.forIdle(); // wait for the processor to become idle
            this.state = "stopped";
        }
    }
}
