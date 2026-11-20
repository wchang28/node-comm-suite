import { EventEmitter } from 'events';
import {
    JobId,
    TaskIndex,
    CPUs,
    NodeId,
    TaskItem,
    TaskItems,
    PID,
    CPUUsageByNode,
    TaskLauncherAckError,
    INodeTaskDispatcher
} from '../types.js';
import {getRandomInt, generateGuid, error_message} from '../../utils.js';
import {RequestError as SSERequestError, ConnectionManagerError} from "../../sse-request/types.js";
import assert from 'assert';

export type TaskDispatchError = TaskLauncherAckError | SSERequestError | ConnectionManagerError;

export interface DispatchResultRow {
    j: JobId;
    t: TaskIndex;
    nodeId: NodeId;
    startTime: number;      // start dispatch time
    endTime: number | null; // end dispatch time 
    durationMS: number | null;  // duration of the dispatch in milliseconds
    pid?: PID | null;
    error?: TaskDispatchError | null;
};

export type DispatchErrors = CPUUsageByNode;

export interface DispatchSessionJSON {
    sessionId: string;
    numTasks: number;
    numCompleted: number;
    completed: boolean;
    startTime: number;      // start dispatch session time
    endTime: number | null; // end dispatch session time 
    durationMS: number | null;  // duration of the dispatch session in milliseconds
    rows: DispatchResultRow[];
}

export interface DispatchSessionEvents {
    "node-to-be-dispatched-one-task": (nodeId: NodeId, taskItem: TaskItem) => void;
    "cpus-usage-assignment": (cpusUsage: CPUUsageByNode) => void;   // emitted only once when the CPU usage is assigned at the start of the dispatch session
    "one-task-dispatched": (row: DispatchResultRow) => void;
    "session-completed": (sessionJSON: DispatchSessionJSON) => void;    // emitted only once when the dispatch session is completed
    "change": () => void;
}

export class DispatchSession extends EventEmitter {
    private get initSessionJSON() : DispatchSessionJSON{
        return {
            sessionId: generateGuid(),
            numTasks: 0,
            numCompleted: 0,
            completed: false,
            startTime: Date.now(),
            endTime: null,
            durationMS: null,
            rows: []    // number of rows = numTasks, will be filled later
        };
    }
    private _dispatchSessionJSON: DispatchSessionJSON = this.initSessionJSON;
    constructor(private _dispatcher: INodeTaskDispatcher) {
        super();
    }
    private onOneTaskDispatchCompleted(row: DispatchResultRow, errors: DispatchErrors, sessionCompleteResolve: (value: DispatchErrors) => void, pid: PID | null, error?: TaskDispatchError | null) {
        row.endTime = Date.now();
        row.durationMS = row.endTime - row.startTime;
        row.pid = pid;
        row.error = error || null;
        this.emit("one-task-dispatched", row);   
        const json = this._dispatchSessionJSON;
        json.numCompleted++;
        if (json.numCompleted === json.numTasks) {
            json.endTime = Date.now();
            json.durationMS = json.endTime - json.startTime;
            json.completed = true;
            this.emit("session-completed", json);
            sessionCompleteResolve(errors); // call the session completion resolver to resolve the session completion promise
        }
        this.emit("change");
    }
    // call this function to start dispatching tasks to nodes
    dispatch(usableCPUs: CPUs, tasks: TaskItems) {
        assert(tasks !== null && tasks != undefined && tasks.length > 0, 'No tasks to dispatch');
        assert(usableCPUs.length > 0, 'No usable CPUs to dispatch tasks to');
        assert(tasks.length <= usableCPUs.length, 'More tasks to dispatch than usable CPUs');
        this._dispatchSessionJSON = this.initSessionJSON;
        this._dispatchSessionJSON.numTasks = tasks.length;
        this._dispatchSessionJSON.rows = new Array(tasks.length).fill(null);
        const me = this;
        const completionPromise = new Promise<DispatchErrors>((resolve) => {
            const cpusUsage: CPUUsageByNode = {};
            const errors: DispatchErrors = {};
            const cpus = [...usableCPUs];   // make a copy of usable CPUs
            for (let i = 0; i < tasks.length; i++) {    // for each task to be dispatched
                const taskItem = tasks[i];
                const {j, t} = taskItem;
                const indexPicked = getRandomInt(cpus.length - 1);
                const nodeId = cpus[indexPicked];
                cpus.splice(indexPicked, 1);
                cpusUsage[nodeId] = (cpusUsage[nodeId] || 0) + 1;   // increase CPU usage on the node
                me._dispatchSessionJSON.rows[i] = {
                    j,
                    t,
                    nodeId,
                    startTime: Date.now(),
                    endTime: null,
                    durationMS: null,
                    pid: null,
                    error: null
                };
                me.emit("change");
                me.emit("node-to-be-dispatched-one-task", nodeId, taskItem);
                const row = me._dispatchSessionJSON.rows[i];
                me._dispatcher.dispatchTaskToNode(nodeId, taskItem)
                .then((pid) => {
                    me.onOneTaskDispatchCompleted(row, errors, resolve, pid, null);
                }).catch((err) => { // if dispatching the task fails, we consider it as a dispatch error for that node, increase the error count for that node, and complete the task dispatch with the error
                    errors[nodeId] = (errors[nodeId] || 0) + 1;
                    const message = error_message(err);
                    me.onOneTaskDispatchCompleted(row, errors, resolve, null, message as TaskDispatchError);
                });
            }
            assert(cpus.length >= 0, 'leftover usable CPUs should be >= 0');  // leftover usable CPUs should be >= 0
            me.emit("cpus-usage-assignment", cpusUsage);
        });
        return completionPromise;
    }
    get sessionId(): string {
        return this._dispatchSessionJSON.sessionId;
    }
    get numCompleted(): number {
        return this._dispatchSessionJSON.numCompleted;
    }
    get numTasks(): number {
        return this._dispatchSessionJSON.numTasks;
    }
    get completed(): boolean {
        return this._dispatchSessionJSON.completed;    
    }
    get startTime(): number {
        return this._dispatchSessionJSON.startTime;
    }
    get endTime(): number | null {
        return this._dispatchSessionJSON.endTime;
    }
    get durationMS(): number | null {
        return this._dispatchSessionJSON.durationMS;
    }
    toJSON(): DispatchSessionJSON {
        return this._dispatchSessionJSON;
    }
    on<K extends keyof DispatchSessionEvents>(
        event: K,
        listener: DispatchSessionEvents[K]
    ): this {
        return super.on(event, listener);
    }
    off<K extends keyof DispatchSessionEvents>(
        event: K,
        listener: DispatchSessionEvents[K]
    ): this {
        return super.off(event, listener);
    }
    once<K extends keyof DispatchSessionEvents>(
        event: K,
        listener: DispatchSessionEvents[K]
    ): this {
        return super.once(event, listener);
    }
    emit<K extends keyof DispatchSessionEvents>(
        event: K,
        ...args: Parameters<DispatchSessionEvents[K]>
    ): boolean {
        return super.emit(event, ...args);
    }
}