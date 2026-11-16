import { EventEmitter } from 'events';
import {
    TaskItem,
    PID,
    JobId,
    TaskIndex,
    TaskLauncherAckError
} from "../type.js";
import { spawn, ChildProcessWithoutNullStreams } from 'child_process';
import { PassThrough } from 'stream';
import {text} from 'stream/consumers';
import treeKill from 'tree-kill';
import { error_message } from '../utils.js';

export interface TaskExecParams {
    cmd: string;
    env?: {[key: string]: string} | null;
    stdin?: string | null;
}

export interface TaskExecParamsInfo {
    jobAborted: boolean;
    execParams: TaskExecParams;
}

export interface TaskExecResult {
    code: number;
    stdout: string;
    stderr: string;
}

export type TaskExecDBOpType = "get-task-exec-params" | "mark-task-finished";

export interface IJobTaskExecDB {
    getTaskExecParams(taskItem: TaskItem): Promise<TaskExecParamsInfo>;
    markTaskAsFinished(taskItem: TaskItem, pid: PID, result: TaskExecResult): Promise<void>;
}

export interface TaskTrackerEvents {
    "finished": [result: TaskExecResult];
}

export interface TaskTrackerJSON {
    j: JobId;
    t: TaskIndex;
    command: string;
    pid: PID | null;
    error: string | null;
    code: number | null;
    running: boolean;
    startTime: number;
    endTime: number | null;
    durationMS: number | null;
}

export class TaskTracker extends EventEmitter {
    private _startTime: number = Date.now();
    private _endTime: number | null = null;
    private _durationMS: number | null = null;
    private _child: ChildProcessWithoutNullStreams;
    private _error: Error | null = null;
    private _code: number | null = null;
    private _stdout: string | null = null;
    private _stderr: string | null = null;
    constructor(public taskItem: TaskItem, private execParams: TaskExecParams) {
        super();
        this._child = this.launchTask();
    }
    once<K extends keyof TaskTrackerEvents>(
        event: K,
        listener: (...args: TaskTrackerEvents[K]) => void
    ): this {
        return super.once(event, listener);
    }
    emit<K extends keyof TaskTrackerEvents>(
        event: K,
        ...args: TaskTrackerEvents[K]
    ): boolean {
        return super.emit(event, ...args);
    }
    private launchTask(): ChildProcessWithoutNullStreams {
        const {cmd: command, env, stdin} = this.execParams;
        const envir: {[key: string]: string} = {
            "JOB_ID": this.taskItem.j.toString(),
            "TASK_INDEX": this.taskItem.t.toString(),
        };
        if (env) {
            for (const [key, value] of Object.entries(env)) {
                envir[key] = value;
            }
        }
        const child = spawn(command, {shell: true, env: envir});
        const me = this;
        child.on('error', (error) => {
            me._error = error;
        });
        child.stdin.setDefaultEncoding('utf-8');
        child.stdout.setEncoding('utf-8');
        child.stderr.setEncoding('utf-8');
        if (stdin) {
            child.stdin.write(stdin);
            child.stdin.end();
        }
        const stdoutStringStream = new PassThrough();
        const stderrStringStream = new PassThrough();
        child.stdout.pipe(stdoutStringStream);
        child.stderr.pipe(stderrStringStream);
        child.on('close', async (code) => {
            me._endTime = Date.now();
            me._durationMS = me._endTime - me._startTime;
            // !!! on Linux code can be null if the process was killed by a signal, or a number if it exited normally !!!
            // !!! on Windows code is always a number, even if the process was killed by a signal (in that case, code will be 1) !!!
            if (code === null) {    // on linux, if the process was killed by a signal, code will be null and signal will be set to the name of the signal that killed the process
                code = 1;
            }
            const stdout = await text(stdoutStringStream);
            const stderr = await text(stderrStringStream);
            me._code = code;
            me._stdout = stdout;
            me._stderr = stderr;
            this.emit("finished", {code, stdout, stderr});
        });
        return child;
    }
    get startTime(): number {
        return this._startTime;
    }
    get endTime(): number | null {
        return this._endTime;
    }
    get durationMS(): number | null {
        return this._durationMS;
    }
    get jobId() : JobId {
        return this.taskItem.j;
    }
    get taskIndex() : TaskIndex {
        return this.taskItem.t;
    }
    get command() : string {
        return this.execParams.cmd;
    }
    get env() : {[key: string]: string} | undefined | null {
        return this.execParams.env;
    }
    get stdin() : string | null | undefined {
        return this.execParams.stdin;
    }
    get pid(): PID | null {
        return (this._child.pid ? this._child.pid : null);
    }
    get code(): number | null {
        return this._code;
    }
    get stdout(): string | null {
        return this._stdout;
    }
    get stderr(): string | null {
        return this._stderr;
    }
    get finished(): boolean {
        return this._code !== null;
    }
    get running(): boolean {
        return this._code === null;
    }
    get error(): string | null {
        return (this._error ? this._error.message : null);
    }
    // kill the process. If the process is already finished, do nothing.
    // throws Error()
    kill(): Promise<void> {
        if (this.running && this.pid !== null) {
            return new Promise<void>((resolve, reject) => {
                treeKill(this.pid!, (error) => {
                    if (error) {
                        reject(error);
                    } else {
                        resolve();
                    }
                });
            });
        } else {
            return Promise.resolve();
        }
    }
    toJSON(): TaskTrackerJSON {
        return {
            j: this.jobId,
            t: this.taskIndex,
            command: this.command,
            pid: this.pid,
            error: this.error,
            code: this.code,
            running: this.running,
            startTime: this._startTime,
            endTime: this._endTime,
            durationMS: this._durationMS,
        };
    }
}

export interface TasksTrackerEvents {
    "task-launched": [j: JobId, t: TaskIndex, pid: PID];
    "task-finished": [j: JobId, t: TaskIndex, pid: PID, durationMS: number, code: number, stdout: string, stderr: string];
    "change": [];
    "db-op-error": [op: TaskExecDBOpType, error: string, j: JobId, t: TaskIndex];
    "task-launch-error": [j: JobId, t: TaskIndex, errorType: TaskLauncherAckError, cause?: string | null];
}

export interface TasksTrackerJSON {
    numRunningTasks: number;
    runningTasks: TaskTrackerJSON[];
}

export class TasksTracker extends EventEmitter {
    private _trackersByPid: Map<PID, TaskTracker> = new Map<PID, TaskTracker>();
    private _trackersByTaskItem: Map<JobId, Map<TaskIndex, TaskTracker>> = new Map<JobId, Map<TaskIndex, TaskTracker>>();
    private _trackers: TaskTracker[] = [];
    constructor(private jobTaskExecDB: IJobTaskExecDB) {
        super();
    }
    on<K extends keyof TasksTrackerEvents>(
        event: K,
        listener: (...args: TasksTrackerEvents[K]) => void
    ): this {
        return super.on(event, listener);
    }
    emit<K extends keyof TasksTrackerEvents>(
        event: K,
        ...args: TasksTrackerEvents[K]
    ): boolean {
        return super.emit(event, ...args);
    }
    private sortTrackers() {
        this._trackers.sort((a, b) => {
            if (a.jobId !== b.jobId) {
                return parseInt(a.jobId) - parseInt(b.jobId);
            } else {
                return a.taskIndex - b.taskIndex;
            }
        });
    }
    private addTaskTracker(tracker: TaskTracker) {
        const jobId = tracker.jobId;
        const taskIndex = tracker.taskIndex;
        const pid = tracker.pid!;
        this._trackers.push(tracker);
        this.sortTrackers();
        this._trackersByPid.set(pid, tracker);
        let taskIndexToTracker = this._trackersByTaskItem.get(jobId);
        if (!taskIndexToTracker) {
            taskIndexToTracker = new Map<TaskIndex, TaskTracker>();
            this._trackersByTaskItem.set(jobId, taskIndexToTracker);
        }
        taskIndexToTracker.set(taskIndex, tracker);
    }
    private removeTaskTracker(tracker: TaskTracker) {
        const jobId = tracker.jobId;
        const taskIndex = tracker.taskIndex;
        const pid = tracker.pid!;
        this._trackers = this._trackers.filter(t => t !== tracker);
        this.sortTrackers();
        this._trackersByPid.delete(pid);
        const taskIndexToTracker = this._trackersByTaskItem.get(jobId);
        if (taskIndexToTracker) {
            taskIndexToTracker.delete(taskIndex);
            if (taskIndexToTracker.size === 0) {
                this._trackersByTaskItem.delete(jobId);
            }
        }
    }
    private async getTaskExecParams(taskItem: TaskItem): Promise<TaskExecParamsInfo> {
        try {
            return await this.jobTaskExecDB.getTaskExecParams(taskItem);
        } catch (error) {
            const message = error_message(error, "Failed to get task execution parameters");
            this.emit("db-op-error", "get-task-exec-params", message, taskItem.j, taskItem.t);
            throw new Error(message);
        }
    }
    private onTaskLaunched(tracker: TaskTracker) {
        this.addTaskTracker(tracker);
        this.emit("task-launched", tracker.jobId, tracker.taskIndex, tracker.pid!);
        this.emit("change");
    }
    private async onTaskFinished(tracker: TaskTracker, result: TaskExecResult) {
        tracker.removeAllListeners();
        this.removeTaskTracker(tracker);
        try {
            await this.jobTaskExecDB.markTaskAsFinished(tracker.taskItem, tracker.pid!, result);
        } catch (error) {
            const message = error_message(error, "Failed to mark task as finished");
            this.emit("db-op-error", "mark-task-finished", message, tracker.jobId, tracker.taskIndex);
        }
        this.emit("task-finished", tracker.jobId, tracker.taskIndex, tracker.pid!, tracker.durationMS!, result.code, result.stdout, result.stderr);
        this.emit("change");
    }
    getTrackerByPid(pid: PID): TaskTracker | undefined {
        return this._trackersByPid.get(pid);
    }
    getTrackerByTaskItem(taskItem: TaskItem): TaskTracker | undefined {
        const taskIndexToTracker = this._trackersByTaskItem.get(taskItem.j);
        if (taskIndexToTracker) {
            return taskIndexToTracker.get(taskItem.t);
        } else {
            return undefined;
        }
    }
    // kill the task by pid. If the task is already finished, do nothing.
    // throws Error()
    async killTaskByPid(pid: PID): Promise<void> {
        const tracker = this.getTrackerByPid(pid);
        if (tracker) {
            await tracker.kill();
        }
    }
    // kill the task by task item. If the task is already finished, do nothing.
    // throws Error()
    async killTaskByTaskItem(taskItem: TaskItem): Promise<void> {
        const tracker = this.getTrackerByTaskItem(taskItem);
        if (tracker) {
            await tracker.kill();
        }
    }
    // kill multiple tasks by their pids. If a task is already finished, do nothing for that task.
    // throws Error()
    async killProcesses(pids: PID[]): Promise<void> {
        await Promise.all(pids.map(pid => this.killTaskByPid(pid)));
    }
    // kill all tasks for a given job. If a task is already finished, do nothing for that task.
    // returns the number of tasks killed,  throws no error
    async killJobProcesses(jobId: JobId) {
        const taskIndexToTracker = this._trackersByTaskItem.get(jobId);
        if (taskIndexToTracker) {
            const trackersForJob = Array.from(taskIndexToTracker.values());
            const promises = trackersForJob.map(async (tracker) => {
                try {
                    await tracker.kill();
                    return true;
                } catch (_) {
                    return false;
                }
            });
            const killedFlags = await Promise.all(promises);
            const numProcessesKilled = killedFlags.filter(killed => killed).length;
            return numProcessesKilled;
        }
        else {
            return 0;
        }
    }
    // emits a task launch error event and returns Error() object.
    private onTaskLaunchError(taskItem: TaskItem, errorType: TaskLauncherAckError, cause?: string | null): Error {
        this.emit("task-launch-error", taskItem.j, taskItem.t, errorType, cause);
        return cause ? new Error(errorType, {cause}) : new Error(errorType);
    }
    // launch a task and return it's process id, throws Error() if failed to launch the task
    async lanuchTask(taskItem: TaskItem): Promise<PID> {
        let jobAborted = false;
        let execParams: TaskExecParams | null = null;
        try {
            const ret = await this.getTaskExecParams(taskItem);
            jobAborted = ret.jobAborted;
            execParams = ret.execParams;
        } catch (error) {
            const cause = error_message(error);
            throw this.onTaskLaunchError(taskItem, "fail-to-get-task-exec-params", cause);
        }
        if (jobAborted) {
            const cause = "Job abortion detected when getting task execution parameters";
            throw this.onTaskLaunchError(taskItem, "job-abortion-detected", cause);
        } else if (!execParams) {
            const cause = "Task execution parameters is null";
            throw this.onTaskLaunchError(taskItem, "bad-task-exec-params", cause);
        } else if (!execParams.cmd) {
            const cause = "Missing field 'cmd' in task execution parameters";
            throw this.onTaskLaunchError(taskItem, "bad-task-exec-params", cause);
        } else {
            const tracker = new TaskTracker(taskItem, execParams!);
            if (tracker.pid == null) {
                const cause = tracker.error;
                throw this.onTaskLaunchError(taskItem, "fail-to-launch-task", cause);
            } else {
                const pid = tracker.pid!;
                this.onTaskLaunched(tracker);
                const me = this;
                tracker.once("finished", async (result) => {
                    try {await me.onTaskFinished(tracker, result);} catch (_) {}
                });
                return pid;
            }
        }
    }
    get numRunningTasks(): number {
        return this._trackers.length;
    }
    toJSON(): TasksTrackerJSON {
        return {
            numRunningTasks: this.numRunningTasks,
            runningTasks: this._trackers.map(tracker => tracker.toJSON()),
        };
    }
}