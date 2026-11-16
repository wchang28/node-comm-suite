import {Client, ClientState} from "../sse-request/client.js";
import {RequestContent, RequestResponse, ConnectionId, RequestId} from "../sse-request/types.js";
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
} from "../type.js";
import {error_message, generateGuid} from "../utils.js";
import { EventEmitter } from 'events';
import os from 'os';
import fs from 'fs';
import express from 'express';
import morgan from 'morgan';
import helmet from 'helmet';
import { OSUtils } from 'node-os-utils';
import { IJobTaskExecDBPool } from "./db-pools.js";
import { Pooling } from "../pooling.js";
import { assert } from "console";

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
    "task-dispatch-received": [j: JobId, t: TaskIndex];
    "task-launched": [j: JobId, t: TaskIndex, pid: PID];
    "task-finished": [j: JobId, t: TaskIndex, pid: PID, durationMS: number, code: number, stdout: string, stderr: string];
    "job-processes-kill-received": [jobId: JobId];
    "job-processes-killed": [jobId: JobId, numProcesses: number];
    "change": [];
    "db-op-error": [op: TaskExecDBOpType, error: string, j: JobId, t: TaskIndex];
    "task-launch-error": [j: JobId, t: TaskIndex, errorType: TaskLauncherAckError, cause?: string | null];
    "notify-dispatcher-error": [error: string];
    "listening": [listenAddr: string];
    "client-state-change": [newState: ClientState];
    "client-ready": [];
    "connection-id-change": [oldConnId: ConnectionId, newConnId: ConnectionId];
    "message": [data: any, event?: string, id?: string];
    "connection-id": [connId: ConnectionId];
    "ping": [timestamp: number];
    "request": [connId: ConnectionId, reqId: RequestId, content: RequestContent];
    "request-ack-error": [connId: ConnectionId, reqId: RequestId, error: string];
    "request-acked": [connId: ConnectionId, reqId: RequestId, response: RequestResponse, reply: any];
    "system-load-pooled": [info: SystemLoadingInfo];
    "system-load-monitor-error": [error: Error];
}

export class TaskLauncherApp extends EventEmitter implements INodeIdentitySource {
    public _app: express.Express = express();
    public sessionId: NodeSessionId = generateGuid();
    public hostname: NodeName = os.hostname();
    public listenHost: string;
    public listenPort: number;
    public systemLoadPoolingIntervalMS: number;
    public availableCPUs: number;
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
        availableCPUs?: number | null,
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
        this.availableCPUs = this.calculateAvailableCPUs(reservedCPUs, availableCPUs);
        this._localBaseUrl = `http://127.0.0.1:${this.listenPort}`;
        this.mainDiskDrivePath = this.getMainDiskDrivePath(mainDiskDrivePath);
        const me = this;
        // SSE request client initialization
        /////////////////////////////////////////////////////////////////////////////////
        const sseClient = new Client(sseUrl, async (request: RequestContent) => {
            const {path, method, body} = request;
            if (path === "/identify" && method === "GET") {
                return await me.handleNodeIdentify();
            } else if (path === "/task" && method === "PUT") {
                return await me.handleLaunchNewTask(body as TaskItem);
            } else {
                return await me.forwardRequestViaLocalFetching(request);
            }
        });
        sseClient.on("state-change", (newState) => {
            me.emit("client-state-change", newState);
        }).on("ready", () => {
            me.emit("client-ready");
        }).on("connection-id-change", (oldConnId, newConnId) => {
            me.emit("connection-id-change", oldConnId, newConnId);
        }).on("change", () => {
            me.emit("change");
        }).on("message", (data, event, id) => {
            me.emit("message", data, event, id);
        }).on("connection-id", (connId) => {
            me.emit("connection-id", connId);
        }).on("ping", (timestamp) => {
            me.emit("ping", timestamp);
        }).on("request", (connId, reqId, content) => {
            me.emit("request", connId, reqId, content);
        }).on("request-ack-error", (connId, reqId, error) => {
            me.emit("request-ack-error", connId, reqId, error);
        }).on("request-acked", (connId, reqId, response, reply) => {
            me.emit("request-acked", connId, reqId, response, reply);
        });
        this._sseClient = sseClient;
        /////////////////////////////////////////////////////////////////////////////////

        // tasks tracker initialization
        /////////////////////////////////////////////////////////////////////////////////
        const tasksTracker = new TasksTracker(new JobTaskExecDBWrapper(dbPool,  this.sessionId, this));
        tasksTracker.on("task-launched", (j, t, pid) => {
            me.emit("task-launched", j, t, pid);
        }).on("task-finished", async (j, t, pid, durationMS, code, stdout, stderr) => {
            await me.notifyDispatcherTaskFinished(j, t, pid, code);    // notify the dispatcher that the task has finished
            me.emit("task-finished", j, t, pid, durationMS, code, stdout, stderr);
        }).on("task-launch-error", (j, t, errorType, cause) => {
            me.emit("task-launch-error", j, t, errorType, cause);
        }).on("db-op-error", (op, error, j, t) => {
            me.emit("db-op-error", op, error, j, t);
        }).on("change", () => {
            me.emit("change");
        });
        this._tasksTracker = tasksTracker;
        /////////////////////////////////////////////////////////////////////////////////

        // system load monitoring initialization
        /////////////////////////////////////////////////////////////////////////////////
        const systemLoadMonitoring = new Pooling<SystemLoadingInfo>(async () => {
            const currentLoading = await me.getSystemLoading();
            const sessionId = me.sessionId;
            const hostname = me.hostname;
            await systemLoadingLogger.log(sessionId, hostname, currentLoading);
            return currentLoading;
        }, this.systemLoadPoolingIntervalMS);
        systemLoadMonitoring.on("callback-returned", (result, error) => {
            if (error) {
                const message = error_message(error);
                me.emit("system-load-monitor-error", new Error(message));
            } else {
                const info = result! as SystemLoadingInfo;
                me.emit("system-load-pooled", info);
            }
        });
        this._systemLoadMonitoring = systemLoadMonitoring;
        /////////////////////////////////////////////////////////////////////////////////
        this.initExpressApp();  // initialize the express app and define routes
    }
    on<K extends keyof TaskLauncherAppEvents>(
        event: K,
        listener: (...args: TaskLauncherAppEvents[K]) => void
    ): this {
        return super.on(event, listener);
    }
    emit<K extends keyof TaskLauncherAppEvents>(
        event: K,
        ...args: TaskLauncherAppEvents[K]
    ): boolean {
        return super.emit(event, ...args);
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
            availableCPUs: this.availableCPUs,
            currentLoading
        }
        return info;
    }
    get taskUtilization(): TaskUtilizationInfo {
        const maxAllowedRunningTasks = this.availableCPUs;
        const numRunningTasks = this._tasksTracker.numRunningTasks;
        const utilizationPercent = numRunningTasks / maxAllowedRunningTasks * 100.0;
        return {
            maxAllowedRunningTasks,
            numRunningTasks,
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
        // get task utilization inf
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
        const me = this;
        const app = this._app;
        app.disable('x-powered-by');
        app.set('trust proxy', true);
        app.use(helmet());
        app.use(morgan('combined'));
        // for health check
        app.get("/health", async (_, res) => {
            res.status(200).json(await me.getHealthInfo());
        });
        // for system loading info
        app.get("/system-loading", async (_, res) => {
            res.status(200).json(await me.getSystemLoading());
        });
        // for node identify info
        app.get("/identify", async (_, res) => {
            res.status(200).json(await me.getNodeIdentifyInfo());
        });
        const taskRouter = this.createTaskRouter();
        app.use("/task", taskRouter);
        const jobRouter = this.createJobRouter();
        app.use("/job", jobRouter);
    }
    private calculateAvailableCPUs(reservedCPUs?: number | null, availableCPUs?: number | null): number {
        if (typeof availableCPUs === "number" && availableCPUs > 0) {
            return availableCPUs!;
        } else {
            const reserved = ((typeof reservedCPUs === "number" && reservedCPUs >= 0) ? reservedCPUs! : 2);
            const totalCPUs = os.cpus().length;
            const available = totalCPUs - reserved;
            return (available > 0 ? available : 1);
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
            this.emit("notify-dispatcher-error", message);
        }
    }
    // notify the dispatcher that a task is finished. DO NOT throw exception
    async notifyDispatcherTaskFinished(j: JobId, t: TaskIndex, pid: PID, code: number) {
        const payload: TaskFinishedMsgPayload = {j, t, pid, code};
        await this.notifyDispatcher("task-finished", payload);
    }
    // handle the request to launch a new task. DO NOT throw exception
    private async handleLaunchNewTask(taskItem: TaskItem) {
        this.emit("task-dispatch-received", taskItem.j, taskItem.t);
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
        this.emit("job-processes-kill-received", jobId);
        const numProcessesKilled = await this._tasksTracker.killJobProcesses(jobId);
        this.emit("job-processes-killed", jobId, numProcessesKilled);
        return numProcessesKilled;
    }
    // start the app
    start() {
        const me = this;
        // start the app server
        this._app.listen(this.listenPort, this.listenHost, () => {
            // connect to the SSE dispatcher server
            me._sseClient.connect();
            me._systemLoadMonitoring.start();
            me.emit("listening", `http://${this.listenHost}:${this.listenPort}`);
        });
    }
}