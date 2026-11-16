import { JobId, Task, Job } from "../type.js";
import { SupportedDBType } from "../db-types.js";
import assert from "node:assert";
import sql from 'mssql';
import pg from 'pg';

export interface IPoolDataPlaneDBPool {
    createNewJob(userId: string, userName: string, description?: string | null, tag?: string | null): Promise<Job>;
    appendTasksPageToJob(jobId: JobId, tasks: Task[]): Promise<Job>;
    getJob(jobId: JobId): Promise<Job>;
    deleteJob(jobId: JobId): Promise<Job>;
    abortJob(userId: string, userName: string, jobId: JobId): Promise<Job>;
    replicateJob(userId: string, userName: string, jobId: JobId, failedTasksOnly: boolean): Promise<Job>;
}

class PoolDataPlaneDBPoolBase {
    protected handleJobActionResult(rows: Job[], jobId: JobId|null|undefined = null , actiion: string|undefined = "(unknown)") {
        if (rows.length === 0) {    // No rows returned
            if (jobId === null || jobId === undefined)
                throw new Error(`Failed to create new job`);
            else {
                throw new Error(`Invalid job id (${jobId})`);
            }
        }
        assert(rows.length === 1, `Unexpected number of rows (${rows.length}) returned from ${actiion}`);
        return rows[0];
    }
}

/**
 * This class implements the IPoolDataPlaneDBPool interface for Microsoft SQL Server using the 'mssql' library.
 */
class MSSQLPoolDataPlaneDBPool extends PoolDataPlaneDBPoolBase implements IPoolDataPlaneDBPool {
    private _poolReady: Promise<sql.ConnectionPool>;
    constructor(config: sql.config, private _schema: string = "dbo") {
        super();
        this._poolReady = sql.connect(config);
    }
    async createNewJob(userId: string, userName: string, description?: string | null, tag?: string | null) {
        const pool = await this._poolReady;
        const result = await pool.request()
            .input('userId', sql.VarChar(100), userId)
            .input('userName', sql.VarChar(200), userName)
            .input('description', sql.VarChar(250), description ? description : null)
            .input('tag', sql.VarChar(250), tag ? tag : null)
            .execute(`${this._schema}.stp_job_create_new`);
        const rows = result.recordset as Job[];
        return this.handleJobActionResult(rows, null, "createNewJob");
    }
    async appendTasksPageToJob(jobId: JobId, tasks: Task[]) {
        const pool = await this._poolReady;
        const result = await pool.request()
            .input('jobId', sql.BigInt, jobId)
            .input('tasks', sql.VarChar(sql.MAX), JSON.stringify(tasks))
            .execute(`${this._schema}.stp_job_append_tasks_page`);
        const rows = result.recordset as Job[];
        return this.handleJobActionResult(rows, jobId, 'appendTasksPageToJob');
    }
    async getJob(jobId: JobId) {
        const pool = await this._poolReady;
        const result = await pool.request()
            .input('jobId', sql.BigInt, jobId)
            .execute(`${this._schema}.stp_job_get`);
        const rows = result.recordset as Job[];
        return this.handleJobActionResult(rows, jobId, 'getJob');
        
    }
    async deleteJob(jobId: JobId) {
        const pool = await this._poolReady;
        const result = await pool.request()
            .input('jobId', sql.BigInt, jobId)
            .execute(`${this._schema}.stp_job_delete`);
        const rows = result.recordset as Job[];
        return this.handleJobActionResult(rows, jobId, 'deleteJob');
    }
    async abortJob(userId: string, userName: string,jobId: JobId) {
        const pool = await this._poolReady;
        const result = await pool.request()
            .input('userId', sql.VarChar(100), userId)
            .input('userName', sql.VarChar(200), userName)
            .input('jobId', sql.BigInt, jobId)
            .execute(`${this._schema}.stp_job_abort`);
        const rows = result.recordset as Job[];
        return this.handleJobActionResult(rows, jobId, 'abortJob');
    }
    async replicateJob(userId: string, userName: string, jobId: JobId, failedTasksOnly: boolean) {
        const pool = await this._poolReady;
        const result = await pool.request()
            .input('userId', sql.VarChar(100), userId)
            .input('userName', sql.VarChar(200), userName)
            .input('jobId', sql.BigInt, jobId)
            .input('failedTasksOnly', sql.Bit, failedTasksOnly)
            .execute(`${this._schema}.stp_job_replicate`);
        const rows = result.recordset as Job[];
        return this.handleJobActionResult(rows, jobId, 'replicateJob');
    }
}

/**
 * This class implements the IPoolDataPlaneDBPool interface for PostgreSQL using the 'pg' library.
 */
class PostgresPoolDataPlaneDBPool extends PoolDataPlaneDBPoolBase implements IPoolDataPlaneDBPool {
    private _pool: pg.Pool;
    constructor(config: pg.PoolConfig, private _schema: string = "public") {
        super();
        this._pool = new pg.Pool(config);
    }
    async createNewJob(userId: string, userName: string, description?: string | null, tag?: string | null)  {
        const result = await this._pool.query(`CALL ${this._schema}.stp_job_create_new($1, $2, $3, $4)`, [userId, userName, description ? description : null, tag ? tag : null]);
        const rows = result.rows as Job[];
        return this.handleJobActionResult(rows, null, 'createNewJob');
    }
    async appendTasksPageToJob(jobId: JobId, tasks: Task[]) {
        const result = await this._pool.query(`CALL ${this._schema}.stp_job_append_tasks_page($1, $2)`, [jobId, JSON.stringify(tasks)]);
        const rows = result.rows as Job[];
        return this.handleJobActionResult(rows, jobId, 'appendTasksPageToJob');
    }
    async getJob(jobId: JobId) {
        const result = await this._pool.query(`CALL ${this._schema}.stp_job_get($1)`, [jobId]);
        const rows = result.rows as Job[];
        return this.handleJobActionResult(rows, jobId, 'getJob');
    }
    async deleteJob(jobId: JobId) {
        const result = await this._pool.query(`CALL ${this._schema}.stp_job_delete($1)`, [jobId]);
        const rows = result.rows as Job[];
        return this.handleJobActionResult(rows, jobId, 'deleteJob');
    }
    async abortJob(userId: string, userName: string, jobId: JobId) {
        const result = await this._pool.query(`CALL ${this._schema}.stp_job_abort($1, $2, $3)`, [userId, userName, jobId]);
        const rows = result.rows as Job[];
        return this.handleJobActionResult(rows, jobId, 'abortJob');
    }
    async replicateJob(userId: string, userName: string, jobId: JobId, failedTasksOnly: boolean) {
        const result = await this._pool.query(`CALL ${this._schema}.stp_job_replicate($1, $2, $3, $4)`, [userId, userName, jobId, failedTasksOnly]);
        const rows = result.rows as Job[];
        return this.handleJobActionResult(rows, jobId, 'replicateJob');
    }
}

export function createPoolDataPlaneDBPool(dbType: SupportedDBType, config: sql.config | pg.PoolConfig, schema: string): IPoolDataPlaneDBPool {
    if (dbType === "mssql") {
        return new MSSQLPoolDataPlaneDBPool(config as sql.config, schema);
    } else if (dbType === "postgresql") {
        return new PostgresPoolDataPlaneDBPool(config as pg.PoolConfig, schema);
    } else {
        throw new Error(`Unsupported database type: ${dbType}`);
    }
}
