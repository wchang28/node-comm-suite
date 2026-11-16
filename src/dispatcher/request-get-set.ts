import type { Request } from 'express';
import type { IPoolDataPlaneDBPool } from './db-pools.js';
import type { User, Job } from '../type.js';
import type { IDispatcherAbstraction, SelfFetchFunction } from './types.js';

export class RequestGetSet {
    constructor(private _request: Request) {
    }

    get poolDataPlaneDBPool(): IPoolDataPlaneDBPool {
        return (this._request as any as { poolDataPlaneDBPool: IPoolDataPlaneDBPool }).poolDataPlaneDBPool;
    }
    set poolDataPlaneDBPool(pool: IPoolDataPlaneDBPool) {
        (this._request as any as { poolDataPlaneDBPool: IPoolDataPlaneDBPool }).poolDataPlaneDBPool = pool;
    }
    get user(): User {
        return (this._request as any as { user: User }).user;
    }
    set user(user: User) {
        (this._request as any as { user: User }).user = user;
    }
    get dispatcher(): IDispatcherAbstraction {
        return (this._request as any as { dispatcher: IDispatcherAbstraction }).dispatcher;
    }
    set dispatcher(dispatcher: IDispatcherAbstraction) {
        (this._request as any as { dispatcher: IDispatcherAbstraction }).dispatcher = dispatcher;
    }
    get job(): Job {
        return (this._request as any as { job: Job }).job;
    }
    set job(job: Job) {
        (this._request as any as { job: Job }).job = job;
    }
    get fetchSelf(): SelfFetchFunction {
        return (this._request as any as { fetchSelf: SelfFetchFunction }).fetchSelf;
    }
    set fetchSelf(fetchSelf: SelfFetchFunction) {
        (this._request as any as { fetchSelf: SelfFetchFunction }).fetchSelf = fetchSelf;
    }
}
