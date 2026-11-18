import EventEmitter from 'events';
import {IPoolDataPlaneDBPool} from './db-pools.js';
import {Pooling} from '../../pooling.js';
import type {Job} from '../types.js';

export interface MostRecentJobsMonitorEvents {
    "pooled": (result: Job[]) => void;
    "error": (error: any) => void;
    "change": (jobs: Job[]) => void;
}

export class MostRecentJobsMonitor extends EventEmitter {
    private readonly pooling_: Pooling<Job[]>;
    private currentJSON: string;
    constructor(dbPool: IPoolDataPlaneDBPool, limit: number = 300, poolingIntervalMS: number = 500) {
        super();
        this.currentJSON = "[]";
        const pooling = new Pooling<Job[]>(async () => await dbPool.getMostRecentJobs(limit), poolingIntervalMS);
        pooling.on("callback-returned", (result, error) => {
            if (!error) {
                const jobs: Job[] = result;
                this.emit("pooled", jobs);
                const updatedJSON = JSON.stringify(jobs);
                if (updatedJSON != this.currentJSON) {
                    this.currentJSON = updatedJSON;
                    this.emit("change", jobs);
                }
            } else {
                this.emit("error", error);
            }
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
    start() {
        this.pooling_.start();
    }
    async stop() {
        await this.pooling_.stop();
    }
    get mostRecentJobs(): Job[] {
        return JSON.parse(this.currentJSON) as Job[];
    }
}