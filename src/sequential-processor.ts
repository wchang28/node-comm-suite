import { EventEmitter } from "events";
import { randomUUID } from "crypto";

export type Processor<I, O> = (input: I) => O | Promise<O>;

interface Job<I, O> {
    tag: string;
    input: I;
    resolve: (o: O) => void;
    reject: (e: unknown) => void;
}

export interface SequentialProcessorEvents<O> {
    "completed": [tag: string, output: O | null, err: unknown];
}

export class SequentialProcessor<I, O> extends EventEmitter {
    private queue_: Job<I, O>[] = [];
    private processing_ = false;

    constructor(private readonly processor_: Processor<I, O>) {
        super();
    }

    on<K extends keyof SequentialProcessorEvents<O>>(event: K, listener: (...args: SequentialProcessorEvents<O>[K]) => void): this {
        return super.on(event, listener);
    }
    emit<K extends keyof SequentialProcessorEvents<O>>(event: K, ...args: SequentialProcessorEvents<O>[K]): boolean {
        return super.emit(event, ...args);
    }

    get processing(): boolean {
        return this.processing_;
    }
    get pending(): number {
        return this.queue_.length;
    }
    // Submit a job to the queue and return a promise that resolves with the output or rejects with an error.
    submit(input: I): Promise<O> {
        return new Promise<O>((resolve, reject) => {
            this.queue_.push({ tag: randomUUID(), input, resolve, reject });
            this.drain();
        });
    }

    private async drain(): Promise<void> {
        if (this.processing_) return;
        this.processing_ = true;
        try {
            let job: Job<I, O> | undefined;
            while ((job = this.queue_.shift())) {
                let ok = false, output: O | undefined, err: unknown;
                try {
                    output = await this.processor_(job.input); // sync throws caught too
                    ok = true;
                } catch (e) {
                    err = e;
                }
                if (ok) job.resolve(output as O); else job.reject(err);
                try {
                    this.emit("completed", job.tag, ok ? output! : null, ok ? null : err);
                } catch { /* a listener's exception must not stall the queue */ }
            }
        } finally {
            this.processing_ = false;
        }
    }
}
