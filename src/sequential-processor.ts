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
    }
    dequeue(): Job<I, O> | undefined {
        const job = this.jobs_.shift();
        if (job) {
            this.safeEmit("dequeue", job);
        }
        return job;
    }
    get length(): number {
        return this.jobs_.length;
    }
    get jobs(): readonly Job<I, O>[] {
        return this.jobs_;
    }
}

export interface SequentialProcessorEvents<I, O> {
    "change": () => void;
    "processing": (input: I) => void;
}

export class SequentialProcessor<I, O> extends EventEmitter {
    private queue_: Queue<I, O> = new Queue<I, O>();
    private processingJob_: Job<I, O> | null = null;
    constructor(private readonly processor_: Processor<I, O>) {
        super();
        this.queue_.on("enqueue", () => {
            this.safeEmit("change");
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
    get processing(): boolean {
        return (this.processingJob_ != null);
    }
    get processingInput(): I | null {
        return (this.processingJob_ ? this.processingJob_.input : null);
    }
    private set processingJob(newJob: Job<I, O> | null) {
        const oldJob = this.processingJob_;
        if (oldJob != newJob) {
            this.processingJob_ = newJob;
            this.safeEmit("change");
            const goingFromProcessingToIdle = (oldJob != null && newJob == null);
            const goingFromIdleToProcessing = (oldJob == null && newJob != null);
            if (goingFromProcessingToIdle) {
                this.drain();
            } else if (goingFromIdleToProcessing) {
                this.safeEmit("processing", newJob!.input);
            }
        }
    }
    get idle(): boolean {
        return (this.processingJob_ == null);
    }
    get pending(): number {
        return this.queue_.length;
    }
    // Submit a job to the queue and return a promise that resolves with the output or rejects with an error.
    submit(input: I): Promise<O> {
        return new Promise<O>((resolve, reject) => {
            this.queue_.enqueue({ input, resolve, reject });
        });
    }
    private drain() {
        if (this.idle) {    // if currently idle, start processing the next job in the queue if there is one
            const job = this.queue_.dequeue();
            if (job) {  // if there is a job, start processing it
                this.processingJob = job; // Idle -> Processing
                //this.processor_(job.input)
                new Promise<O>((resolve) => resolve(this.processor_(job.input)))
                .then((output) => {
                    job.resolve(output);
                })
                .catch((err) => {
                    job.reject(err);
                })
                .finally(() => {
                    this.processingJob = null; // Processing -> Idle
                });
            }
        }
    }
    get queuedInputs(): I[] {
        return this.queue_.jobs.map(job => job.input);
    }
}
