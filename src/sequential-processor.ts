import { EventEmitter } from "events";

export type Processor<I, O> = (input: I) => Promise<O>;

interface Job<I, O> {
    input: I;
    resolve: (o: O) => void;
    reject: (e: unknown) => void;
}

interface QueueEvents<I, O> {
    "enqueue": [Job<I, O>];
    "dequeue": [Job<I, O>]; 
}

class Queue<I, O> extends EventEmitter {
    private jobs: Job<I, O>[] = [];
    constructor() {
        super();
    }
    on<K extends keyof QueueEvents<I, O>>(event: K, listener: (...args: QueueEvents<I, O>[K]) => void): this {
        return super.on(event, listener);
    }
    emit<K extends keyof QueueEvents<I, O>>(event: K, ...args: QueueEvents<I, O>[K]): boolean {
        return super.emit(event, ...args);
    }
    enqueue(job: Job<I, O>): void {
        this.jobs.push(job);
        this.emit("enqueue", job);
    }
    dequeue(): Job<I, O> | undefined {
        const job = this.jobs.shift();
        if (job) {
            this.emit("dequeue", job);
        }
        return job;
    }
    get length(): number {
        return this.jobs.length;
    }
}

export class SequentialProcessor<I, O> {
    private queue_: Queue<I, O> = new Queue();
    private processing_ = false;

    constructor(private readonly processor_: Processor<I, O>) {
        this.queue_.on("enqueue", () => {
            this.drain();
        });
    }

    get processing(): boolean {
        return this.processing_;
    }
    set processing(value: boolean) {
        const oldValue = this.processing_;
        if (oldValue != value) {
            this.processing_ = value;
            const goingFromProcessingToIdle = (oldValue && !value); 
            if (goingFromProcessingToIdle) {
                this.drain();
            }
        }
    }
    get idle(): boolean {
        return !this.processing_;
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
                this.processing = true; // Idle -> Processing
                this.processor_(job.input)
                .then((output) => {
                    job.resolve(output);
                })
                .catch((err) => {
                    job.reject(err);
                })
                .finally(() => {
                    this.processing = false; // Processing -> Idle
                });
            }
        }
    }
}
