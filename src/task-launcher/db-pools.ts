import { JobId, NodeId, NodeName, NodeSessionId, PID, TaskIndex } from "../type.js";
import { SupportedDBType } from "../db-types.js";
import { TaskExecParamsInfo} from "./tasks-tracker.js";
import assert from "node:assert";
import sql from 'mssql';
import pg from 'pg';

export interface IJobTaskExecDBPool {
    acknowledgeTaskDispatch(j: JobId, t: TaskIndex, nodeId: NodeId, hostname: NodeName, nodeSessionId: NodeSessionId): Promise<TaskExecParamsInfo>;
    markTaskAsFinished(j: JobId, t: TaskIndex, pid: PID, code: number, stdout: string, stderr: string): Promise<void>;
}

interface TaskExecParamsInfoRow {
    jobAborted: boolean;
    cmd: string;
    envJSON: string | null;
    stdin: string | null;
};

class JobTaskExecDBPoolBase {
    protected handleAckResult(rows: TaskExecParamsInfoRow[]) {
        assert(rows.length > 0, "No rows returned from acknowledgeTaskDispatch");
        const {jobAborted, cmd, envJSON, stdin} = rows[0];
        let env: {[key: string]: string} | null = null;
        if (envJSON) {
            try {
                env = JSON.parse(envJSON);
            } catch (e) {}
        }
        const ret: TaskExecParamsInfo = {
            jobAborted,
            execParams: {
                cmd,
                env: env || undefined,
                stdin: stdin || undefined
            }
        };
        return ret;
    }
    protected handleMarkFinishedResult(code: number, rows: {code: number}[]) {
        assert(rows.length > 0, "No rows returned from markTaskAsFinished");
        const row = rows[0];
        assert(row.code === code, `Finished code ${row.code} does not match expected code ${code}`);
    }
}

/**
 * This class implements the IJobTaskExecDBPool interface for Microsoft SQL Server using the 'mssql' library.
 */
class MSSQLJobTaskExecDBPool extends JobTaskExecDBPoolBase implements IJobTaskExecDBPool {
    private _poolReady: Promise<sql.ConnectionPool>;
    constructor(config: sql.config, private _schema: string = "dbo") {
        super();
        this._poolReady = sql.connect(config);
    }
    async acknowledgeTaskDispatch(j: JobId, t: TaskIndex, nodeId: NodeId, hostname: NodeName, nodeSessionId: NodeSessionId) {
        const pool = await this._poolReady;
        const result = await pool.request()
            .input('j', sql.BigInt, j)
            .input('t', sql.Int, t)
            .input('nodeId', sql.VarChar(50), nodeId)
            .input('hostname', sql.VarChar(250), hostname)
            .input('nodeSessionId', sql.VarChar(50), nodeSessionId)
            .execute(`${this._schema}.stp_job_task_exec_ack`);
        const rows = result.recordset as TaskExecParamsInfoRow[];
        return this.handleAckResult(rows);
    }
    async markTaskAsFinished(j: JobId, t: TaskIndex, pid: PID, code: number, stdout: string, stderr: string) {
        const pool = await this._poolReady;
        const result = await pool.request()
            .input('j', sql.BigInt, j)
            .input('t', sql.Int, t)
            .input('pid', sql.Int, pid)
            .input('code', sql.BigInt, code)
            .input('stdout', sql.VarChar(sql.MAX), stdout)
            .input('stderr', sql.VarChar(sql.MAX), stderr)
            .execute(`${this._schema}.stp_job_task_exec_finish`);
        const rows = result.recordset as {code: number}[];
        this.handleMarkFinishedResult(code, rows);
    }
}

/**
 * This class implements the IJobTaskExecDBPool interface for PostgreSQL using the 'pg' library.
 */
class PostgresJobTaskExecDBPool extends JobTaskExecDBPoolBase implements IJobTaskExecDBPool {
    private _pool: pg.Pool;
    constructor(config: pg.PoolConfig, private _schema: string = "public") {
        super();
        this._pool = new pg.Pool(config);
    }
    async acknowledgeTaskDispatch(j: JobId, t: TaskIndex, nodeId: NodeId, hostname: NodeName, nodeSessionId: NodeSessionId) {
        const result = await this._pool.query(`CALL ${this._schema}.stp_job_task_exec_ack($1, $2, $3, $4, $5)`, [j, t, nodeId, hostname, nodeSessionId]);
        const rows = result.rows as TaskExecParamsInfoRow[];
        return this.handleAckResult(rows);
    }
    async markTaskAsFinished(j: JobId, t: TaskIndex, pid: PID,code: number, stdout: string, stderr: string) {
        const result = await this._pool.query(`CALL ${this._schema}.stp_job_task_exec_finish($1, $2, $3, $4, $5, $6)`, [j, t, pid, code, stdout, stderr]);
        const rows = result.rows as {code: number}[];
        this.handleMarkFinishedResult(code, rows);
    }
}

export function createDBPool(dbType: SupportedDBType, config: sql.config | pg.PoolConfig, schema: string): IJobTaskExecDBPool {
    if (dbType === "mssql") {
        return new MSSQLJobTaskExecDBPool(config as sql.config, schema);
    } else if (dbType === "postgresql") {
        return new PostgresJobTaskExecDBPool(config as pg.PoolConfig, schema);
    } else {
        throw new Error(`Unsupported database type: ${dbType}`);
    }
}