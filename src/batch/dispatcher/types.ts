import { JobId } from '../types.js';

export interface IDispatcherAbstraction {
    queueJob(jobId: JobId, numTasks: number): Promise<void>;
    killJob(jobId: JobId): Promise<void>;
}

export type SelfFetchFunction = (path: string, init?: RequestInit) => Promise<globalThis.Response>;