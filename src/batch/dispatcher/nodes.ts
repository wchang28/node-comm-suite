import { EventEmitter } from 'node:events';
import {
    NodeId,
    NodeName,
    NodeSessionId,
    SystemLoadStatus,
    SystemLoadingInfo,
    CPUs,
    CPUUsageByNode,
    TaskItem,
    TaskLaunchSuccessPayload,
    INodeTaskDispatcher,
    NodeHealthInfo,
    NodeInfoRow,
    PID,
    TaskFinishedMsgPayload,
    JobId,
    KillJobProcessesResult
} from '../types.js';
import {ConnectionManagerError} from "../../sse-request/types.js"; 
import {Connection} from "../../sse-request/connection-manager.js";
import assert from "assert";

export interface NodeEvents {
    "change": [];
    "enabled": [];
    "disabled": [];
    "task-finished": [payload: TaskFinishedMsgPayload]; // fired when a task has finished on this node
    "more-usable-task-slots-become-available": [];   // fired when more enabled task slots become available for this node
}

export class Node extends EventEmitter {
    private _enabled: boolean = true;
    private _numAvailableTaskSlots: number;
    constructor(private _connection: Connection, public id: NodeId, public sessionId: NodeSessionId, public hostname: NodeName, public totalTaskSlots: number, private _currentLoading: SystemLoadingInfo) {
        super();
        assert(this.id, "Node ID must be defined");
        assert(this.id === this._connection.connId, `Node ID ${this.id} must match connection ID ${this._connection.connId}`);
        assert(this.sessionId, `Node session ID must be defined (node=${id})`);
        assert(this.hostname, `Node hostname must be defined (node=${id})`);
        assert(this.totalTaskSlots > 0, `Total CPUs must be greater than 0 (node=${id}, totalTaskSlots=${totalTaskSlots})`);
        this._numAvailableTaskSlots = this.totalTaskSlots;
        const me = this;
        this._connection.on("message", (event: string, data?: any) => {
            if (event === "task-finished") {    // one task is finished on this node
                me.increaseAvailableTaskSlots(1);   // increase the number of available task slots by 1
                me.emit('task-finished', data as TaskFinishedMsgPayload);
            }
        });
    }
    get numAvailableTaskSlots(): number {
        return this._numAvailableTaskSlots;
    }
    get numUsedTaskSlots(): number {
        return this.totalTaskSlots - this._numAvailableTaskSlots;
    }
    get currentLoading(): SystemLoadingInfo {
        return this._currentLoading;
    }
    // update the current loading info of this node, and emit 'change' event if the loading info has changed
    set currentLoading(loading: SystemLoadingInfo) {
        this._currentLoading = loading;
        this.emit('change');
    }
    get cpuUsagePercent(): number {
        return this._currentLoading.cpuUsagePercent;
    }
    get memUsagePercent(): number {
        return this._currentLoading.memUsagePercent;
    }
    get loadingScore(): number {
        return this._currentLoading.loadingScore;
    }
    get loadStatus(): SystemLoadStatus {
        return this._currentLoading.loadStatus;
    }
    decreaseAvailableTaskSlots(count: number): void {
        assert(count > 0, "Task slot decrease count must be positive");
        const old = this._numAvailableTaskSlots; 
        this._numAvailableTaskSlots -= count;
        if (this._numAvailableTaskSlots < 0) {
            this._numAvailableTaskSlots = 0;
        }
        if (this._numAvailableTaskSlots !== old) {
            this.emit('change');
        }
    }
    increaseAvailableTaskSlots(count: number): void {
        assert(count > 0, "Task slot increase count must be positive");
        const old = this._numAvailableTaskSlots;
        this._numAvailableTaskSlots += count;
        if (this._numAvailableTaskSlots > this.totalTaskSlots) {
            this._numAvailableTaskSlots = this.totalTaskSlots;
        }
        if (this._numAvailableTaskSlots !== old) {
            if (this._enabled && this._numAvailableTaskSlots > old) {
                this.emit('more-usable-task-slots-become-available');
            }
            this.emit('change');
        }
    }
    get utilizationPercent(): number {
        return (this.numUsedTaskSlots / this.totalTaskSlots) * 100.0;
    }
    get utilization(): string {
        const s =`${this.numUsedTaskSlots}/${this.totalTaskSlots}=${this.utilizationPercent.toFixed(2)}%`;
        return s;
    }
    get enabled(): boolean {
        return this._enabled;
    }
    set enabled(value: boolean) {
        if (this._enabled !== value) {
            this._enabled = value;
            if (this._enabled && this._numAvailableTaskSlots > 0) {  // disable -> enable
                this.emit('more-usable-task-slots-become-available');
            }
            if (this._enabled) {
                this.emit('enabled');
            } else {
                this.emit('disabled');
            }
            this.emit('change');
        }
    }
    get totalEnabledTaskSlots(): number {
        return this.enabled ? this.totalTaskSlots : 0;
    }
    get numUsableTaskSlots(): number {
        return this.enabled ? this._numAvailableTaskSlots : 0;
    }
    get usableTaskSlots(): CPUs {
        const cpus: CPUs = [];
        if (this.enabled) {
            for (let i = 0; i < this._numAvailableTaskSlots; i++) {
                cpus.push(this.id);
            }
        }
        return cpus;
    }
    get remoteSocketAddress(): string {
        return this._connection.remoteSocketAddress;
    }
    async request<T>(url: string, method: string, body?: any, headers?: {[key: string]: string}): Promise<T> {
        return this._connection.request<T>(url, method, body, headers);
    }
    async dispatchTask(taskItem: TaskItem) {
        const { pid } = await this.request<TaskLaunchSuccessPayload>("/task", "PUT", taskItem);
        return pid;
    }
    // kill multiple processes on the node by their PIDs, throws no exception
    async killProcesses(pids: PID[]) {
        try {
            await this.request<any>("/task/kill-processes", "POST", pids);
        } catch (_) {}
    }
    // kill all processes associated with a specific job on the node, returns the number of processes killed
    // throws no exception
    async killJobProcesses(jobId: JobId) {
        try {
            const {numProcessesKilled} = await this.request<KillJobProcessesResult>(`/job/${jobId}`, "DELETE");
            return numProcessesKilled;
        } catch (_) {
            return 0;
        }
    }
    async getHealth() {
        return this.request<NodeHealthInfo>("/health", "GET");
    }
    toJSON(): NodeInfoRow {
        return {
            id: this.id,
            sessionId: this.sessionId,
            hostname: this.hostname,
            remoteSocketAddress: this.remoteSocketAddress,
            enabled: this._enabled,
            totalTaskSlots: this.totalTaskSlots,
            numUsedTaskSlots: this.numUsedTaskSlots,
            numUsableTaskSlots: this.numUsableTaskSlots,
            utilization: this.utilization,
            cpuUsagePercent: this.cpuUsagePercent,
            memUsagePercent: this.memUsagePercent,
            loadingScore: this.loadingScore,
            loadStatus: this.loadStatus
        }
    }
    on<K extends keyof NodeEvents>(
        event: K,
        listener: (...args: NodeEvents[K]) => void
    ): this {
        return super.on(event, listener);
    }
    emit<K extends keyof NodeEvents>(
        event: K,
        ...args: NodeEvents[K]
    ): boolean {
        return super.emit(event, ...args);
    }
}

export interface NodeTrackerEvents {
    "change": [];
    "usable-cpus-become-available": [];   // fired when usable CPUs become available (from unavailable to available)
    "usable-cpus-increased": [oldUsableCpus: number];   // fired when more usable CPUs become available
    "node-added": [node: Node];
    "node-removed": [node: Node];
    "nodes-present": [];
    "nodes-empty": [];
    "task-finished-on-node": [node: Node, payload: TaskFinishedMsgPayload];
}

export class NodesTracker extends EventEmitter implements INodeTaskDispatcher {
    private _nodes: Map<NodeId, Node> = new Map<NodeId, Node>();
    private _nodeList: Node[] = [];
    private _numUsableTaskSlots: number = 0;
    constructor() {
        super();
    }
    private calculateNumberOfUsableTaskSlots(): number {
        let total = 0;
        for (const node of this._nodes.values()) {
            total += node.numUsableTaskSlots;
        }
        return total;
    }
    addNode(node: Node): void {
        const oldNumNodes = this._nodes.size;
        this._nodes.set(node.id, node);
        this._nodeList.push(node);
        const me = this;
        node.on('change', () => {
            const oldUsableTaskSlots = me._numUsableTaskSlots;
            const newUsableTaskSlots = me.calculateNumberOfUsableTaskSlots();
            if (newUsableTaskSlots !== oldUsableTaskSlots) {
                me._numUsableTaskSlots = newUsableTaskSlots;
            }
            if (oldUsableTaskSlots === 0 && newUsableTaskSlots > 0) {
                me.emit('usable-cpus-become-available');
            }
            if (newUsableTaskSlots > oldUsableTaskSlots) {
                me.emit('usable-cpus-increased', oldUsableTaskSlots);
            }
            me.emit('change'); 
        }).on('task-finished', (payload) => {
            me.emit('task-finished-on-node', node, payload);
        });
        const oldUsableTaskSlots = this._numUsableTaskSlots;
        const newUsableTaskSlots = this.calculateNumberOfUsableTaskSlots();
        if (newUsableTaskSlots !== oldUsableTaskSlots) {
            this._numUsableTaskSlots = newUsableTaskSlots;
        }
        if (oldUsableTaskSlots === 0 && newUsableTaskSlots > 0) {
            this.emit('usable-cpus-become-available');
        }
        if (newUsableTaskSlots > oldUsableTaskSlots) {
            this.emit('usable-cpus-increased', oldUsableTaskSlots);
        }
        this.emit('node-added', node);
        this.emit('change');
        const newNumNodes = this._nodes.size;
        if (oldNumNodes == 0 && newNumNodes > 0) {
            // The first node has been added
            this.emit('nodes-present');
        }
    }
    // get a node by its ID, throw error if not found
    getNode(nodeId: NodeId): Node {
        const node =this._nodes.get(nodeId)
        if (!node) {
            const message: ConnectionManagerError = "bad-connection-id";
            const cause = `Node with ID ${nodeId} not found`;
            throw new Error(message, {cause});
        }
        return node;
    }
    removeNode(nodeId: NodeId): void {
        const oldNumNodes = this._nodes.size;
        const node = this._nodes.get(nodeId);
        if (node) {
            this._nodes.delete(nodeId);
            const index = this._nodeList.indexOf(node);
            if (index >= 0) {
                this._nodeList.splice(index, 1);
            }
            node.removeAllListeners();
            this._numUsableTaskSlots = this.calculateNumberOfUsableTaskSlots();
            this.emit('node-removed', node);
            this.emit('change'); 
        }
        const newNumNodes = this._nodes.size;
        if (oldNumNodes > 0 && newNumNodes === 0) {
            this.emit('nodes-empty');
        }
    }
    setNodeEnabled(nodeId: NodeId, enabled: boolean): void {
        const node = this._nodes.get(nodeId);
        if (node) {
            node.enabled = enabled;
        }
    }
    get numNodes(): number {
        return this._nodes.size;
    }
    get numUsableTaskSlots(): number {
        return this._numUsableTaskSlots;
    }
    get hasUsableTaskSlots(): boolean {
        return this._numUsableTaskSlots > 0;
    }
    get usableTaskSlots(): CPUs {
        const cpus: CPUs = [];
        for (const node of this._nodes.values()) {  // for each node
            cpus.push(...node.usableTaskSlots);
        }
        return cpus;
    }
    // call this function to reflect the CPU usage on nodes after dispatching tasks
    increaseCPUUsageOnNodes(cpuUsage: CPUUsageByNode): void {
        for (const [nodeId, count] of Object.entries(cpuUsage)) {
            const node = this._nodes.get(nodeId);
            if (node) {
                node.decreaseAvailableTaskSlots(count);
            }
        }
    }
    // call this function to reflect the CPU usage on nodes after failing to dispatch tasks
    decreaseCPUUsageOnNodes(cpuUsage: CPUUsageByNode): void {
        for (const [nodeId, count] of Object.entries(cpuUsage)) {
            const node = this._nodes.get(nodeId);
            if (node) {
                node.increaseAvailableTaskSlots(count);
            }
        }
    }
    async dispatchTaskToNode(nodeId: NodeId, taskItem: TaskItem) {
        return await this.getNode(nodeId).dispatchTask(taskItem);
    }
    // kill multiple processes on the node by their PIDs, throws no exception
    async killProcessesOnNode(nodeId: NodeId, pids: PID[]) {
        await this.getNode(nodeId).killProcesses(pids);
    }
    // kill all processes associated with a specific job on all nodes, returns the number of processes killed
    // throws no exception
    async killJobProcesses(jobId: JobId) {
        const nodes = Array.from(this._nodes.values());
        const promises = nodes.map(node => node.killJobProcesses(jobId));
        const results = await Promise.all(promises);
        const numProcessesKilled = results.reduce((accumulator, current) => accumulator + current, 0);
        return numProcessesKilled;
    }
    async getNodeHealth(nodeId: NodeId) {
        return await this.getNode(nodeId).getHealth();
    }
    get nodesSessionIds(): NodeSessionId[] {
        return this._nodeList.map(node => node.sessionId);
    }
    toJSON(): NodeInfoRow[] {
        return this._nodeList.map(node => node.toJSON());
    }
    on<K extends keyof NodeTrackerEvents>(
        event: K,
        listener: (...args: NodeTrackerEvents[K]) => void
    ): this {
        return super.on(event, listener);
    }
    emit<K extends keyof NodeTrackerEvents>(
        event: K,
        ...args: NodeTrackerEvents[K]
    ): boolean {
        return super.emit(event, ...args);
    }
}