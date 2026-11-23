import {Client, ClientState} from "../../sse-request/client.js";
import {RequestContent, RequestResponse, ConnectionId, RequestId} from "../../sse-request/types.js";
import {
    IJobTaskExecDB,
    TaskExecResult,
    TaskExecDBOpType,
    TaskTracker,
    TasksTracker
} from "./tasks-tracker.js";
import {
    JobId,
    TaskIndex,
    TaskItem,
    PID,
    NodeSessionId,
    TaskLauncherAckError,
    NodeIdentifyInfo,
    NodeToDispatcherEventType,
    TaskLaunchSuccessPayload,
    TaskLaunchErrorPayload,
    NodeName,
    TaskLauncherClientInfo,
    TaskUtilizationInfo,
    CPUUsageInfo,
    MemoryInfo,
    ProcessInfo,
    NetworkInterface,
    NodeHealthInfo,
    DiskInfo,
    SystemLoadingInfo,
    SystemLoadStatus,
    NodeId,
    ISystemLoadingLogger,
    TaskFinishedMsgPayload,
    KillJobProcessesResult,
} from "../types.js";
import {error_message, generateGuid} from "../../utils.js";
import { EventEmitter } from 'node:events';
import os from 'node:os';
import fs from 'node:fs';
import express from 'express';
import morgan from 'morgan';
import helmet from 'helmet';
import { OSUtils } from 'node-os-utils';
import { IJobTaskExecDBPool } from "./db-pools.js";
import { Pooling } from "../../pooling.js";
import { assert } from "node:console";
import http from 'node:http';

interface NodeIdentity {
    id: NodeId;
    hostname: NodeName;
}

interface INodeIdentitySource {
    getNodeIdentity(): Promise<NodeIdentity>;
}

class JobTaskExecDBWrapper implements IJobTaskExecDB {
    constructor(private _dbPool: IJobTaskExecDBPool, private _nodeSessionId: NodeSessionId, private _nodeIdentitySrc: INodeIdentitySource) {
    }
    async getTaskExecParams(taskItem: TaskItem) {
        const {j, t} = taskItem;
        const {id: nodeId, hostname} = await this._nodeIdentitySrc.getNodeIdentity();
        return await this._dbPool.acknowledgeTaskDispatch(j, t, nodeId, hostname, this._nodeSessionId);
    }
    async markTaskAsFinished(taskItem: TaskItem, pid: PID, result: TaskExecResult): Promise<void> {
        const {j, t} = taskItem;
        const {code, stdout, stderr} = result;
        return await this._dbPool.markTaskAsFinished(j, t, pid, code, stdout, stderr);
    }
}

export interface TaskLauncherAppEvents {
    "task-dispatch-received": (j: JobId, t: TaskIndex) => void;
    "task-launched": (j: JobId, t: TaskIndex, pid: PID) => void;
    "task-finished": (j: JobId, t: TaskIndex, pid: PID, durationMS: number, code: number, stdout: string, stderr: string) => void;
    "job-processes-kill-received": (jobId: JobId) => void;
    "job-processes-killed": (jobId: JobId, numProcesses: number) => void;
    "change": () => void;
    "db-op-error": (op: TaskExecDBOpType, error: string, j: JobId, t: TaskIndex) => void;
    "task-launch-error": (j: JobId, t: TaskIndex, errorType: TaskLauncherAckError, cause?: string | null) => void;
    "notify-dispatcher-error": (error: string) => void;
    "started": (listenAddr: string) => void;
    "stopped": () => void;
    "client-state-change": (newState: ClientState) => void;
    "client-ready": () => void;
    "connection-id-change": (oldConnId: ConnectionId, newConnId: ConnectionId) => void;
    "message": (data: any, event?: string, id?: string) => void;
    "connection-id": (connId: ConnectionId) => void;
    "ping": (timestamp: number) => void;
    "request": (connId: ConnectionId, reqId: RequestId, content: RequestContent) => void;
    "request-ack-error": (connId: ConnectionId, reqId: RequestId, error: string) => void;
    "request-acked": (connId: ConnectionId, reqId: RequestId, response: RequestResponse, reply: any) => void;
    "system-load-pooled": (info: SystemLoadingInfo) => void;
    "system-load-monitor-error": (error: Error) => void;
}

export class TaskLauncherApp extends EventEmitter implements INodeIdentitySource {
    private _app: express.Express = express();
    private _server: http.Server | null = null;
    public sessionId: NodeSessionId = generateGuid();
    public hostname: NodeName = os.hostname();
    public listenHost: string;
    public listenPort: number;
    public systemLoadPoolingIntervalMS: number;
    public totalTaskSlots: number;  // total number of task slots available on this node = total number of tasks are allowed be run on this node concurrently
    private _localBaseUrl: string;
    public mainDiskDrivePath: string | null;
    private _sseClient: Client;
    private _tasksTracker: TasksTracker;
    private _systemLoadMonitoring: Pooling<SystemLoadingInfo>;
    constructor(
        sseUrl: string,
        dbPool: IJobTaskExecDBPool,
        systemLoadingLogger: ISystemLoadingLogger,
        listenHost?: string | null,
        listenPort?: number | null,
        systemLoadPoolingIntervalMS?: number | null,
        reservedCPUs?: number | null,
        totalTaskSlots?: number | null,
        mainDiskDrivePath?: string | null
    ) {
        super();
        this.listenHost = listenHost || "0.0.0.0";
        this.listenPort = listenPort || 8080;
        this.systemLoadPoolingIntervalMS = systemLoadPoolingIntervalMS || 1000;
        assert(sseUrl, `sseUrl must be defined`);
        assert(this.listenHost, `listenHost must be defined`);
        assert(this.listenPort > 0 && this.listenPort < 65536, `listenPort (${this.listenPort}) must be a valid port number between 1 and 65535`);
        assert(this.systemLoadPoolingIntervalMS >= 500, `systemLoadPoolingIntervalMS (${this.systemLoadPoolingIntervalMS}) must be at least 500ms`);
        this.totalTaskSlots = this.calculateTotalTaskSlots(reservedCPUs, totalTaskSlots);
        this._localBaseUrl = `http://127.0.0.1:${this.listenPort}`;
        this.mainDiskDrivePath = this.getMainDiskDrivePath(mainDiskDrivePath);
        // SSE request client initialization
        /////////////////////////////////////////////////////////////////////////////////
        const sseClient = new Client(sseUrl, async (request: RequestContent) => {
            const {path, method, body} = request;
            if (path === "/identify" && method === "GET") {
                return await this.handleNodeIdentify();
            } else if (path === "/task" && method === "PUT") {
                return await this.handleLaunchNewTask(body as TaskItem);
            } else {
                return await this.forwardRequestViaLocalFetching(request);
            }
        });
        sseClient.on("state-change", (newState) => {
            this.safeEmit("client-state-change", newState);
        }).on("ready", () => {
            this.safeEmit("client-ready");
        }).on("connection-id-change", (oldConnId, newConnId) => {
            this.safeEmit("connection-id-change", oldConnId, newConnId);
        }).on("change", () => {
            this.safeEmit("change");
        }).on("message", (data, event, id) => {
            this.safeEmit("message", data, event, id);
        }).on("connection-id", (connId) => {
            this.safeEmit("connection-id", connId);
        }).on("ping", (timestamp) => {
            this.safeEmit("ping", timestamp);
        }).on("request", (connId, reqId, content) => {
            this.safeEmit("request", connId, reqId, content);
        }).on("request-ack-error", (connId, reqId, error) => {
            this.safeEmit("request-ack-error", connId, reqId, error);
        }).on("request-acked", (connId, reqId, response, reply) => {
            this.safeEmit("request-acked", connId, reqId, response, reply);
        });
        this._sseClient = sseClient;
        /////////////////////////////////////////////////////////////////////////////////

        // tasks tracker initialization
        /////////////////////////////////////////////////////////////////////////////////
        const tasksTracker = new TasksTracker(new JobTaskExecDBWrapper(dbPool,  this.sessionId, this));
        tasksTracker.on("task-launched", (j, t, pid) => {
            this.safeEmit("task-launched", j, t, pid);
        }).on("task-finished", async (j, t, pid, durationMS, code, stdout, stderr) => {
            await this.notifyDispatcherTaskFinished(j, t, pid, code);    // notify the dispatcher that the task has finished
            this.safeEmit("task-finished", j, t, pid, durationMS, code, stdout, stderr);
        }).on("task-launch-error", (j, t, errorType, cause) => {
            this.safeEmit("task-launch-error", j, t, errorType, cause);
        }).on("db-op-error", (op, error, j, t) => {
            this.safeEmit("db-op-error", op, error, j, t);
        }).on("change", () => {
            this.safeEmit("change");
        });
        this._tasksTracker = tasksTracker;
        /////////////////////////////////////////////////////////////////////////////////

        // system load monitoring initialization
        /////////////////////////////////////////////////////////////////////////////////
        const systemLoadMonitoring = new Pooling<SystemLoadingInfo>(async () => {
            const currentLoading = await this.getSystemLoading();
            const sessionId = this.sessionId;
            const hostname = this.hostname;
            await systemLoadingLogger.log(sessionId, hostname, currentLoading);
            return currentLoading;
        }, this.systemLoadPoolingIntervalMS);
        systemLoadMonitoring.on("callback-returned", (result, error) => {
            if (error) {
                const message = error_message(error);
                this.safeEmit("system-load-monitor-error", new Error(message));
            } else {
                const info = result! as SystemLoadingInfo;
                this.safeEmit("system-load-pooled", info);
            }
        });
        this._systemLoadMonitoring = systemLoadMonitoring;
        /////////////////////////////////////////////////////////////////////////////////
        this.initExpressApp();  // initialize the express app and define routes
    }
    on<K extends keyof TaskLauncherAppEvents>(
        event: K,
        listener: TaskLauncherAppEvents[K]
    ): this {
        return super.on(event, listener);
    }
    off<K extends keyof TaskLauncherAppEvents>(
        event: K,
        listener: TaskLauncherAppEvents[K]
    ): this {
        return super.off(event, listener);
    }
    emit<K extends keyof TaskLauncherAppEvents>(
        event: K,
        ...args: Parameters<TaskLauncherAppEvents[K]>
    ): boolean {
        return super.emit(event, ...args);
    }
    once<K extends keyof TaskLauncherAppEvents>(
        event: K,
        listener: TaskLauncherAppEvents[K]
    ): this {
        return super.once(event, listener);
    }
    safeEmit<K extends keyof TaskLauncherAppEvents>(
        event: K,
        ...args: Parameters<TaskLauncherAppEvents[K]>
    ): void {
        try {
            this.emit(event, ...args);
        } catch (error) {
            console.error(`TaskLauncherApp "${event}" listener threw:`, error);
        }
    }
    getMainDiskDrivePath(mainDiskDrivePath?: string | null): string | null {
        mainDiskDrivePath = mainDiskDrivePath || "";
        if (!mainDiskDrivePath) {
            if (process.platform === "win32") {
                mainDiskDrivePath = process.env.SystemDrive || "C:";
            } else {
                mainDiskDrivePath = "/mnt/c";
            }
        }
        const exists = fs.existsSync(mainDiskDrivePath); // sanity check
        return (exists ? mainDiskDrivePath! : null);
    }
    async getNodeIdentifyInfo() {
        const currentLoading = await this.getSystemLoading();
        const info: NodeIdentifyInfo = {
            sessionId: this.sessionId,
            hostname: this.hostname,
            totalTaskSlots: this.totalTaskSlots,
            currentLoading
        }
        return info;
    }
    get taskUtilization(): TaskUtilizationInfo {
        const totalTaskSlots = this.totalTaskSlots;
        const usedTaskSlots = this._tasksTracker.numRunningTasks;
        const utilizationPercent = usedTaskSlots / totalTaskSlots * 100.0;
        return {
            totalTaskSlots,
            usedTaskSlots,
            utilizationPercent
        };
    }
    async getHealthInfo(): Promise<NodeHealthInfo> {
        const clientInfo = this.clientInfo;
        const taskUtilization = this.taskUtilization;
        const cpu: CPUUsageInfo = {cpus: os.cpus().length, usagePercent: null};
        const osutils = new OSUtils();
        const cpuUsage = await osutils.cpu.usage();
        if (cpuUsage.success) {
            cpu.usagePercent = cpuUsage.data;
        }
        const totalMem = os.totalmem();
        const freeMem = os.freemem();
        const usedMem = totalMem - freeMem;
        const memUsagePercent = (totalMem > 0 && usedMem >= 0 ? (usedMem / totalMem) * 100.0 : null);
        const memory: MemoryInfo = {
            totalMem,
            freeMem,
            usedMem,
            usagePercent: memUsagePercent
        }
        const {rss, heapTotal, heapUsed} = process.memoryUsage();
        const heapUsagePercent = (heapTotal > 0 && heapUsed >= 0 ? (heapUsed / heapTotal) * 100.0 : null);
        const processInfo: ProcessInfo = {
            rss,
            heapTotal,
            heapUsed,
            heapUsagePercent
        };
        const interfaces = os.networkInterfaces();
        const networkInterfaces: NetworkInterface[] = [];
        for (const [name, infos] of Object.entries(interfaces)) {
            if (infos) {
                for (const info of infos) {
                    if (!info.internal) {
                        networkInterfaces.push({
                            name,
                            family: info.family,
                            address: info.address
                        });
                    }
                }
            }
        }
        let diskInfo: DiskInfo | undefined = undefined;
        if (this.mainDiskDrivePath && fs.existsSync(this.mainDiskDrivePath)) {
            const stats = fs.statfsSync(this.mainDiskDrivePath);
            const blockSize = stats.bsize;
            const totalSpace = stats.blocks * blockSize;    // Total size of the filesystem
            const freeSpace = stats.bfree * blockSize; // Physical free
            const usedSpace = totalSpace - freeSpace;
            const availableSpace = stats.bavail * blockSize; // Available to current user
            const usagePercent = (totalSpace > 0 && usedSpace >= 0 ? (usedSpace / totalSpace) * 100.0 : null);
            diskInfo = {
                totalSpace,
                freeSpace,
                usedSpace,
                availableSpace,
                usagePercent
            };
        }
        return {
            hostname: this.hostname,
            client: clientInfo,
            taskUtilization,
            cpu,
            memory,
            process: processInfo,
            disk: diskInfo,
            interfaces: networkInterfaces
        }
    }
    async getSystemLoading(cpuWeight: number = 0.5, memWeight: number = 0.5) {
        const {utilizationPercent: taskUtilizationPercent} = this.taskUtilization;
        let cpuUsagePercent = taskUtilizationPercent;
        const osutils = new OSUtils();
        const cpuUsage = await osutils.cpu.usage();
        if (cpuUsage.success) {
            cpuUsagePercent = cpuUsage.data;
        }
        const totalMem = os.totalmem();
        const freeMem = os.freemem();
        const usedMem = totalMem - freeMem;
        const memUsagePercent = (usedMem / totalMem) * 100;
        let loadingScore = (cpuUsagePercent * cpuWeight) + (memUsagePercent * memWeight);
        if (loadingScore < 1) loadingScore = 1;
        if (loadingScore > 100) loadingScore = 100;
        const loadStatus: SystemLoadStatus = (loadingScore > 90 ? 'critical' : loadingScore > 75 ? 'high' : 'normal');
        const ret: SystemLoadingInfo = {
            cpuUsagePercent,
            memUsagePercent,
            loadingScore,
            loadStatus
        };
        return ret;
    }
    // create a express router for handling requests related to a running task (identified by PID or jobId+taskIndex in the path), which supports getting information about the running task and killing the running task
    private createRunningTaskObjectRouter(): express.Router {
        const router = express.Router();
        const getTaskTracker = (req: express.Request): TaskTracker => {
            return (req as any).taskTracker as TaskTracker; 
        };
        // get information about this running task
        router.get("/", (req, res) => {
            const tracker = getTaskTracker(req);
            res.status(200).json(tracker.toJSON());
        });
        // kill this running task
        router.delete("/", async (req, res) => {
            const tracker = getTaskTracker(req);
            try {
                await tracker.kill();
                res.status(200).json({});
            } catch(err) {
                const message = error_message(err);
                res.status(500).json({message: `Failed to kill the task. Error: ${message}`});
            }
        });
        return router;
    }
    // "/task" route
    private createTaskRouter(): express.Router {
        const me = this;
        const router = express.Router();
        const runningTaskObjRouter = this.createRunningTaskObjectRouter();
        // get all running tasks
        router.get("/", (_, res) => {
            res.status(200).json(me._tasksTracker.toJSON());
        });
        // get task utilization info
        router.get("/utilization", (_, res) => {
            res.status(200).json(me.taskUtilization);
        });
        // kill multiple running tasks/proceses by their pids
        router.post("/kill-processes", express.json({limit: '999mb'}), async (req, res) => {
            const pids = req.body as PID[];
            try {
                await me._tasksTracker.killProcesses(pids);
            } catch(_) {}
            res.status(200).json({});
        });
        router.use("/:pid",
            (req, res, next) => {
                const pid = parseInt(req.params.pid as any as string) as PID;
                if (isNaN(pid) || pid <= 0) {
                    res.status(400).json({message: `Invalid PID in path: ${req.params.pid}`});
                } else {
                    const tracker = me._tasksTracker.getTrackerByPid(pid);
                    if (tracker) {
                        (req as any).taskTracker = tracker;
                        next();
                    } else {
                        res.status(404).json({message: `No running task found with PID: ${pid}`});
                    }
                }
            },
            runningTaskObjRouter
        );
        return router;
    }
    // "/job" route
    private createJobRouter(): express.Router {
        const me = this;
        const router = express.Router();
        const runningTaskObjRouter = this.createRunningTaskObjectRouter();
        router.delete("/:jobId", async (req, res) => {
            const jobId = req.params.jobId as JobId;
            if (!jobId) {
                res.status(400).json({message: `Invalid JobId in path: ${req.params.jobId}`});
            } else {
                try {
                    const numProcessesKilled = await me.killJobProcesses(jobId);
                    const result: KillJobProcessesResult = {numProcessesKilled};
                    res.status(200).json(result);
                } catch(err) {
                    const message = error_message(err);
                    res.status(500).json({message: `Failed to kill the job processes. Error: ${message}`});
                }
            }
        });
        router.use("/:jobId/task/:taskIndex",
            (req, res, next) => {
                const jobId = req.params.jobId as any as JobId;
                if (!jobId) {
                    res.status(400).json({message: `Invalid JobId in path: ${req.params.jobId}`});
                } else {
                    const taskIndex = parseInt(req.params.taskIndex as any as string) as TaskIndex;
                    if (isNaN(taskIndex) || taskIndex < 0) {
                        res.status(400).json({message: `Invalid TaskIndex in path: ${req.params.taskIndex}`});
                    }
                    else {
                        const tracker = me._tasksTracker.getTrackerByTaskItem({j: jobId, t: taskIndex});
                        if (tracker) {
                            (req as any).taskTracker = tracker;
                            next();
                        } else {
                            res.status(404).json({message: `No running task found with task item: (${jobId}, ${taskIndex})`});
                        }
                    }
                }
            },
            runningTaskObjRouter
        );
        return router;
    }
    private initExpressApp() {
        const app = this._app;
        app.disable('x-powered-by');
        app.set('trust proxy', true);
        app.use(helmet());
        app.use(morgan('combined'));
        // for health check
        app.get("/health", async (_, res) => {
            res.status(200).json(await this.getHealthInfo());
        });
        // for system loading info
        app.get("/system-loading", async (_, res) => {
            res.status(200).json(await this.getSystemLoading());
        });
        // for node identify info
        app.get("/identify", async (_, res) => {
            res.status(200).json(await this.getNodeIdentifyInfo());
        });
        const taskRouter = this.createTaskRouter();
        app.use("/task", taskRouter);
        const jobRouter = this.createJobRouter();
        app.use("/job", jobRouter);
    }
    private calculateTotalTaskSlots(reservedCPUs?: number | null, totalTaskSlots?: number | null): number {
        if (typeof totalTaskSlots === "number" && totalTaskSlots > 0) {
            return totalTaskSlots!;
        } else {
            const reserved = ((typeof reservedCPUs === "number" && reservedCPUs >= 0) ? reservedCPUs! : 2);
            const totalCPUs = os.cpus().length;
            const totalTaskSlots = totalCPUs - reserved;
            return (totalTaskSlots > 0 ? totalTaskSlots : 1);
        }
    }
    private async handleNodeIdentify() {
        const res: RequestResponse = {
            status: 200,
            headers: {"content-type": "application/json"},
            body: await this.getNodeIdentifyInfo()
        };
        return res;
    }
    // forward the request to local app server and return the response. DO NOT throw exception
    private async forwardRequestViaLocalFetching(request: RequestContent) {
        try {
            const {path, method, body: reqBody, headers: hdrs} = request;
            const localUrl = `${this._localBaseUrl}${path}`;
            const headers = hdrs || {};
            if (reqBody) {
                headers['Content-Type'] = 'application/json';
            }
            const reqInit: RequestInit = {
                method,
                headers,
                body: (reqBody ? JSON.stringify(reqBody) : undefined)
            }
            const response = await fetch(localUrl, reqInit);
            const body = await response.json();
            const res: RequestResponse = {
                status: response.status,
                headers: {"content-type": "application/json"},
                body
            };
            return res;
        } catch(err) {
            const message = error_message(err);
            const res: RequestResponse = {
                status: 500,
                headers: {"content-type": "application/json"},
                body: {
                    message
                }
            };
            return res;
        }
    }
    // notify the dispatcher with a message. DO NOT throw exception
    async notifyDispatcher(event: NodeToDispatcherEventType, data?: any) {
        try {
            await this._sseClient.sendMessage(event, data);
        } catch(err) {
            const message = error_message(err);
            this.safeEmit("notify-dispatcher-error", message);
        }
    }
    // notify the dispatcher that a task is finished. DO NOT throw exception
    async notifyDispatcherTaskFinished(j: JobId, t: TaskIndex, pid: PID, code: number) {
        const payload: TaskFinishedMsgPayload = {j, t, pid, code};
        await this.notifyDispatcher("task-finished", payload);
    }
    // handle the request to launch a new task. DO NOT throw exception
    private async handleLaunchNewTask(taskItem: TaskItem) {
        this.safeEmit("task-dispatch-received", taskItem.j, taskItem.t);
        try {
            const pid = await this._tasksTracker.lanuchTask(taskItem);
            const payload: TaskLaunchSuccessPayload = {
                pid
            };
            const res: RequestResponse = {
                status: 200,
                headers: {"content-type": "application/json"},
                body: payload
            };
            return res;
        } catch(err) {
            const error = err as Error;
            const errorType: TaskLauncherAckError = (error.message as TaskLauncherAckError);
            const cause = (error.cause ? error.cause.toString() : null);
            const payload: TaskLaunchErrorPayload = {
                message: errorType,
                cause
            };
            const res: RequestResponse = {
                status: 400,
                headers: {"content-type": "application/json"},
                body: payload
            };
            return res;
        }
    }
    // interface INodeIdentitySource
    async getNodeIdentity(): Promise<NodeIdentity> {
        await this._sseClient.ready
        return {
            id: this._sseClient.connId!,
            hostname: this.hostname
        };
    }
    get connId(): ConnectionId | null {
        return this._sseClient.connId;
    }
    get clientState(): ClientState {
        return this._sseClient.state;
    }
    get clientIsReady(): boolean {
        return this._sseClient.isReady;
    }
    get clientInfo(): TaskLauncherClientInfo {
        return {
            state: this._sseClient.state,
            connId: this._sseClient.connId,
            isReady: this._sseClient.isReady
        };
    }
    async killJobProcesses(jobId: JobId): Promise<number> {
        this.safeEmit("job-processes-kill-received", jobId);
        const numProcessesKilled = await this._tasksTracker.killJobProcesses(jobId);
        this.safeEmit("job-processes-killed", jobId, numProcessesKilled);
        return numProcessesKilled;
    }
    // start the app
    start() {
        // start the app server
        if (this._server == null) {
            this._server = this._app.listen(this.listenPort, this.listenHost, () => {
                // connect to the SSE dispatcher server
                this._sseClient.connect();
                this._systemLoadMonitoring.start();
                this.safeEmit("started", `http://${this.listenHost}:${this.listenPort}`);
            });
        }
    }
    // stop the app server and related components
    async stop() {
        if (this._server != null) {
            await this._systemLoadMonitoring.stop();
            this._sseClient.close();
            const serverStopPromise = new Promise<void>((resolve) => {
                this._server!.close(() => {
                    resolve();
                });
            });
            await serverStopPromise;
            this._server = null;
            this.safeEmit("stopped");
        }
    }
}