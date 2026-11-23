import EventEmitter from 'node:events';
import {IPoolDataPlaneDBPool} from './db-pools.js';
import {Pooling} from '../../pooling.js';
import type {Job} from '../types.js';

export interface MostRecentJobsMonitorEvents {
    "pooled": (result: Job[]) => void;
    "pooling-error": (error: any) => void;
    "change": () => void;
    "started": () => void;
    "stopped": () => void;
}

export interface IMostRecentJobsMonitor {
    on<U extends keyof MostRecentJobsMonitorEvents>(
        event: U, listener: MostRecentJobsMonitorEvents[U]
    ): this;
    off<U extends keyof MostRecentJobsMonitorEvents>(
        event: U, listener: MostRecentJobsMonitorEvents[U]
    ): this;
    emit<U extends keyof MostRecentJobsMonitorEvents>(
        event: U, ...args: Parameters<MostRecentJobsMonitorEvents[U]>
    ): boolean;
    once<U extends keyof MostRecentJobsMonitorEvents>(
        event: U, listener: MostRecentJobsMonitorEvents[U]
    ): this;
    start(): void;
    stop(): Promise<void>;
    toJSON(): Job[];
}

export class MostRecentJobsMonitor extends EventEmitter implements IMostRecentJobsMonitor {
    private readonly pooling_: Pooling<Job[]>;
    private currentJSON: string;
    constructor(dbPool: IPoolDataPlaneDBPool, limit: number = 300, poolingIntervalMS: number = 500) {
        super();
        this.currentJSON = "[]";
        const pooling = new Pooling<Job[]>(async () => await dbPool.getMostRecentJobs(limit), poolingIntervalMS);
        pooling.on("callback-returned", (result, error) => {
            if (!error) {
                const jobs: Job[] = result;
                this.safeEmit("pooled", jobs);
                const updatedJSON = JSON.stringify(jobs);
                if (updatedJSON != this.currentJSON) {
                    this.currentJSON = updatedJSON;
                    this.safeEmit("change");
                }
            } else {
                this.safeEmit("pooling-error", error);
            }
        }).on("started", () => {
            this.safeEmit("started");
        }).on("stopped", () => {
            this.safeEmit("stopped");
        });
        this.pooling_ = pooling;
    }
    on<U extends keyof MostRecentJobsMonitorEvents>(
        event: U, listener: MostRecentJobsMonitorEvents[U]
    ): this {
        return super.on(event, listener);
    }
    off<U extends keyof MostRecentJobsMonitorEvents>(
        event: U, listener: MostRecentJobsMonitorEvents[U]
    ): this {
        return super.off(event, listener);
    }
    emit<U extends keyof MostRecentJobsMonitorEvents>(
        event: U, ...args: Parameters<MostRecentJobsMonitorEvents[U]>
    ): boolean {
        return super.emit(event, ...args);
    }
    once<U extends keyof MostRecentJobsMonitorEvents>(
        event: U, listener: MostRecentJobsMonitorEvents[U]
    ): this {
        return super.once(event, listener);
    }
    private safeEmit<U extends keyof MostRecentJobsMonitorEvents>(
        event: U, ...args: Parameters<MostRecentJobsMonitorEvents[U]>
    ): void {
        try {
            this.emit(event, ...args);
        } catch (err) {
            console.error(`MostRecentJobsMonitor "${event}" listener threw:`, err);
        }
    }
    start() {
        this.pooling_.start();
    }
    async stop() {
        await this.pooling_.stop();
    }
    toJSON(): Job[] {
        return JSON.parse(this.currentJSON) as Job[];
    }
}