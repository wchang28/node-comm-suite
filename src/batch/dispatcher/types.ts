import { JobId, DispatcherJSON } from '../types.js';

export interface IDispatcherAbstraction {
    enable(): Promise<boolean>; // enable the dispatcher
    disable(): Promise<boolean>; // disable the dispatcher
    isEnabled(): Promise<boolean>; // return whether the dispatcher is enabled
    closeQueue(): Promise<boolean>; // close the dispatcher's queue
    openQueue(): Promise<boolean>; // open the dispatcher's queue
    isQueueClosed(): Promise<boolean>; // return whether the dispatcher's queue is closed
    queueJob(jobId: JobId, numTasks: number): Promise<void>; // queue a job with the given number of tasks
    killJob(jobId: JobId): Promise<void>; // kill the specified job
    toJSON(): Promise<DispatcherJSON>; // return the JSON representation of the dispatcher
}

export type SelfFetchFunction = (path: string, init?: RequestInit) => Promise<globalThis.Response>;