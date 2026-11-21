export type JobId = string;
export type TaskIndex = number;
export type TaskItem = { j: JobId; t: TaskIndex };
export type TaskItems = TaskItem[];
export type NodeId = string;
export type NodeName = string;
export type GUID = string;
export type NodeSessionId = GUID;
export type TaskSlots = NodeId[];
export type PID = number;
export type TaskSlotsUsageByNode = {[nodeId: NodeId]: number};

export type TaskLauncherClientInfo = {
    state: string;
    connId: string | null;
    isReady: boolean;
}

export interface TaskUtilizationInfo {
    totalTaskSlots: number;
    numRunningTasks: number;
    utilizationPercent: number;
}

export interface CPUUsageInfo {
    cpus: number;
    usagePercent?: number | null;   
}

export interface MemoryInfo {
    totalMem: number;
    freeMem: number;
    usedMem: number;
    usagePercent?: number | null;
}

export interface ProcessInfo {
    rss: number;
    heapTotal: number;
    heapUsed: number;
    heapUsagePercent?: number | null;
}

export interface DiskInfo {
    totalSpace: number;
    freeSpace: number;
    usedSpace: number;
    availableSpace: number;
    usagePercent?: number | null;
}

export interface NetworkInterface {
    name: string;
    family: string;
    address: string;
}

export interface NodeHealthInfo {
    hostname: string;
    client: TaskLauncherClientInfo;
    taskUtilization: TaskUtilizationInfo;
    cpu: CPUUsageInfo;
    memory: MemoryInfo;
    process: ProcessInfo;
    disk?: DiskInfo;
    interfaces: NetworkInterface[];
}

export type NodeLoadingScore = number;   // 1-100, higher means more loaded
export type SystemLoadStatus = 'normal' | 'high' | 'critical';

export interface SystemLoadingInfo {
    cpuUsagePercent: number;
    memUsagePercent: number;
    loadingScore: NodeLoadingScore;   // 1-100, higher means more loaded
    loadStatus: SystemLoadStatus;
}

export interface NodeIdentifyInfo {
    sessionId: NodeSessionId;
    hostname: NodeName;
    totalTaskSlots: number;  // total number of task slots available on this node (> 0)
    currentLoading: SystemLoadingInfo;
}

export interface NodeInfoRow {
    id: NodeId;
    sessionId: NodeSessionId;
    hostname: NodeName;
    remoteSocketAddress: string;
    enabled: boolean;
    totalTaskSlots: number;
    numUsedTaskSlots: number;
    numUsableTaskSlots: number;
    utilization: string;
    cpuUsagePercent: number;
    memUsagePercent: number;
    loadingScore: number;
    loadStatus: SystemLoadStatus;
}

export type TaskLauncherAckError
= 'fail-to-get-task-exec-params'
| 'job-abortion-detected'
| 'bad-task-exec-params'
| 'fail-to-launch-task'
| 'unknown-error'
;

export interface TaskLaunchSuccessPayload {
    pid: PID;
}

export interface TaskLaunchErrorPayload {
    message: TaskLauncherAckError;
    cause?: string | null;
}

export interface INodeTaskDispatcher {
    dispatchTaskToNode(nodeId: NodeId, taskItem: TaskItem): Promise<PID>;
}

export type NodeToDispatcherEventType = 'task-finished';

export interface TaskFinishedMsgPayload {
    j: JobId;
    t: TaskIndex;
    pid: PID;
    code: number;
}

export interface KillJobProcessesResult {
    numProcessesKilled: number;
}

export interface ISystemLoadingLogger {
    log(sessionId: NodeSessionId, hostname: NodeName, currentLoading: SystemLoadingInfo): Promise<void>;
    get(sessionIds: NodeSessionId[]): Promise<{sessionId: NodeSessionId, currentLoading: SystemLoadingInfo}[]>;
}

export interface SystemLoadingMonitorConfig {
    baseUrl: string;
    poolingIntervalMS: number;
}

export interface User {
    id: string;
    name: string;
}

export interface Task {
    cmd: string;
    tag?: string | null;
    envJSON?: string | null;
    stdin?: string | null;
}

export type JobStatus = 'SUBMITTED' | 'ABORTED' | 'FINISHED' | 'STARTED';

export interface Job {
    jobId: JobId;
    description: string | null;
    tag: string | null;
    userId: string;
    userName: string;
    submitTime: Date | string;
    status: JobStatus;
    numTasks: number;
    numTasksFinished: number;
    startTime: Date | string | null;
    finishTime: Date | string | null;
    durationSeconds: number | null;
    success: boolean | null;
    numTasksSuccess: number;
    finished: boolean | null;
    completePct: number | null;
    abortedById: string | null;
    abortedBy: string | null;
    abortTime: Date | string | null;
}
