import {EventEmitter} from "node:events";
import type {JobId} from "../types.js";
import {SequentialProcessor} from "../../sequential-processor.js";
import {NodesTracker} from "./nodes.js";
import {sleep} from "../../utils.js";

export type JobKillState = "queued" | "killing";

export interface JobKillStatRow {
    jobId: JobId;
    state: JobKillState;
}

export type NumberOfProcessesKilled = number;

export interface NodesJobKillerEvents {
    "change": () => void;
    "killing": (jobId: JobId) => void;
    "killed": (jobId: JobId, numProcessesKilled: NumberOfProcessesKilled) => void;
    "kill-error": (jobId: JobId, err: unknown) => void;
    "kill-retry": (jobId: JobId, retryIndex: number, numProcessesKilled: NumberOfProcessesKilled) => void;
}

export class NodesJobKiller extends EventEmitter {
    private processor_: SequentialProcessor<JobId, NumberOfProcessesKilled>;
    constructor(nodesTracker: NodesTracker, public jobKillRetries: number = 2, public jobKillTryIntervalMS: number = 500) {
        super();
        if (jobKillRetries < 1) {
            throw new Error("jobKillRetries must be at least 1");
        }
        if (jobKillTryIntervalMS < 200) {
            throw new Error("jobKillTryIntervalMS must be at least 200");
        }
        const processor = new SequentialProcessor<JobId, NumberOfProcessesKilled>(async (jobId) => {
            let retries: number = 0;
            let totalProcessKilledCount: NumberOfProcessesKilled = 0;
            while (true) {
                const n = await nodesTracker.killJobProcesses(jobId);
                this.safeEmit("kill-retry", jobId, retries, n);
                totalProcessKilledCount += n;
                retries++;
                if (retries < this.jobKillRetries) {
                    await sleep(this.jobKillTryIntervalMS);
                } else {
                    break;
                }
            }
            return totalProcessKilledCount;
        });
        processor.on("change", () => {
            this.safeEmit("change");
        }).on("processing", (jobId) => {
            this.safeEmit("killing", jobId);
        }).on("processed", (jobId, numProcessesKilled) => {
            this.safeEmit("killed", jobId, numProcessesKilled);
        }).on("process-error", (jobId, err) => {
            this.safeEmit("kill-error", jobId, err);
        });
        this.processor_ = processor;
    }
    on<U extends keyof NodesJobKillerEvents>(
        event: U, listener: NodesJobKillerEvents[U]
    ): this {
        return super.on(event, listener);
    }
    off<U extends keyof NodesJobKillerEvents>(
        event: U, listener: NodesJobKillerEvents[U]
    ): this {
        return super.off(event, listener);
    }
    emit<U extends keyof NodesJobKillerEvents>(
        event: U, ...args: Parameters<NodesJobKillerEvents[U]>
    ): boolean {
        return super.emit(event, ...args);
    }
    once<U extends keyof NodesJobKillerEvents>(
        event: U, listener: NodesJobKillerEvents[U]
    ): this {
        return super.once(event, listener);
    }
    private safeEmit<U extends keyof NodesJobKillerEvents>(
        event: U, ...args: Parameters<NodesJobKillerEvents[U]>
    ): void {
        try {
            this.emit(event, ...args);
        } catch (err) {
            console.error(`NodesJobKiller "${event}" listener threw:`, err);
        }
    }
    async killJob(jobId: JobId) {
        return await this.processor_.submit(jobId);
    }
    get killing() {
        return this.processor_.processing;
    }
    get killingJobId(): JobId | null {
        return this.processor_.processingInput;
    }
    toJSON(): JobKillStatRow[] {
        return this.processor_.toJSON().map(({state:js, input: jobId}) => {
            const state: JobKillState = (js == "queued" ? "queued" : "killing");
            return {state, jobId};
        });
    }
}