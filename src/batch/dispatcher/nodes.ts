import { EventEmitter } from 'events';
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
    "more-usable-cpus-become-available": [];   // fired when more enabled CPUs become available for this node
}

export class Node extends EventEmitter {
    private _enabled: boolean = true;
    private _numAvailableCpus: number;
    constructor(private _connection: Connection, public id: NodeId, public sessionId: NodeSessionId, public hostname: NodeName, public totalCpus: number, private _currentLoading: SystemLoadingInfo) {
        super();
        assert(this.id, "Node ID must be defined");
        assert(this.id === this._connection.connId, `Node ID ${this.id} must match connection ID ${this._connection.connId}`);
        assert(this.sessionId, `Node session ID must be defined (node=${id})`);
        assert(this.hostname, `Node hostname must be defined (node=${id})`);
        assert(this.totalCpus > 0, `Total CPUs must be greater than 0 (node=${id}, totalCpus=${totalCpus})`);
        this._numAvailableCpus = this.totalCpus;
        const me = this;
        this._connection.on("message", (event: string, data?: any) => {
            if (event === "task-finished") {
                me.increaseAvailableCpus(1);
                me.emit('task-finished', data as TaskFinishedMsgPayload);
            }
        });
    }
    get numAvailableCpus(): number {
        return this._numAvailableCpus;
    }
    get numUsedCpus(): number {
        return this.totalCpus - this._numAvailableCpus;
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
    decreaseAvailableCpus(count: number): void {
        assert(count > 0, "CPU decrease count must be positive");
        const old = this._numAvailableCpus; 
        this._numAvailableCpus -= count;
        if (this._numAvailableCpus < 0) {
            this._numAvailableCpus = 0;
        }
        if (this._numAvailableCpus !== old) {
            this.emit('change');
        }
    }
    increaseAvailableCpus(count: number): void {
        assert(count > 0, "CPU increase count must be positive");
        const old = this._numAvailableCpus;
        this._numAvailableCpus += count;
        if (this._numAvailableCpus > this.totalCpus) {
            this._numAvailableCpus = this.totalCpus;
        }
        if (this._numAvailableCpus !== old) {
            if (this._enabled && this._numAvailableCpus > old) {
                this.emit('more-usable-cpus-become-available');
            }
            this.emit('change');
        }
    }
    get utiizationPercent(): number {
        return (this.numUsedCpus / this.totalCpus) * 100.0;
    }
    get utiization(): string {
        const s =`${this.numUsedCpus}/${this.totalCpus}=${this.utiizationPercent.toFixed(2)}%`;
        return s;
    }
    get enabled(): boolean {
        return this._enabled;
    }
    set enabled(value: boolean) {
        if (this._enabled !== value) {
            this._enabled = value;
            if (this._enabled && this._numAvailableCpus > 0) {  // disable -> enable
                this.emit('more-usable-cpus-become-available');
            }
            if (this._enabled) {
                this.emit('enabled');
            } else {
                this.emit('disabled');
            }
            this.emit('change');
        }
    }
    get totalEnabledCpus(): number {
        return this.enabled ? this.totalCpus : 0;
    }
    get numUsableCpus(): number {
        return this.enabled ? this._numAvailableCpus : 0;
    }
    get usableCpus(): CPUs {
        const cpus: CPUs = [];
        if (this.enabled) {
            for (let i = 0; i < this._numAvailableCpus; i++) {
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
            totalCpus: this.totalCpus,
            numUsedCpus: this.numUsedCpus,
            numUsableCpus: this.numUsableCpus,
            utiization: this.utiization,
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
    private _numUsableCpus: number = 0;
    constructor() {
        super();
    }
    private calculateNumberOfUsableCpus(): number {
        let total = 0;
        for (const node of this._nodes.values()) {
            total += node.numUsableCpus;
        }
        return total;
    }
    addNode(node: Node): void {
        const oldNumNodes = this._nodes.size;
        this._nodes.set(node.id, node);
        this._nodeList.push(node);
        const me = this;
        node.on('change', () => {
            const oldUsableCpus = me._numUsableCpus;
            const newUsableCpus = me.calculateNumberOfUsableCpus();
            if (newUsableCpus !== oldUsableCpus) {
                me._numUsableCpus = newUsableCpus;
            }
            if (oldUsableCpus === 0 && newUsableCpus > 0) {
                me.emit('usable-cpus-become-available');
            }
            if (newUsableCpus > oldUsableCpus) {
                me.emit('usable-cpus-increased', oldUsableCpus);
            }
            me.emit('change'); 
        }).on('task-finished', (payload) => {
            me.emit('task-finished-on-node', node, payload);
        });
        const oldUsableCpus = this._numUsableCpus;
        const newUsableCpus = this.calculateNumberOfUsableCpus();
        if (newUsableCpus !== oldUsableCpus) {
            this._numUsableCpus = newUsableCpus;
        }
        if (oldUsableCpus === 0 && newUsableCpus > 0) {
            this.emit('usable-cpus-become-available');
        }
        if (newUsableCpus > oldUsableCpus) {
            this.emit('usable-cpus-increased', oldUsableCpus);
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
            this._numUsableCpus = this.calculateNumberOfUsableCpus();
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
    get numUsableCpus(): number {
        return this._numUsableCpus;
    }
    get hasUsableCpus(): boolean {
        return this._numUsableCpus > 0;
    }
    get usableCpus(): CPUs {
        const cpus: CPUs = [];
        for (const node of this._nodes.values()) {  // for each node
            cpus.push(...node.usableCpus);
        }
        return cpus;
    }
    // call this function to reflect the CPU usage on nodes after dispatching tasks
    increaseCPUUsageOnNodes(cpuUsage: CPUUsageByNode): void {
        for (const [nodeId, count] of Object.entries(cpuUsage)) {
            const node = this._nodes.get(nodeId);
            if (node) {
                node.decreaseAvailableCpus(count);
            }
        }
    }
    // call this function to reflect the CPU usage on nodes after failing to dispatch tasks
    decreaseCPUUsageOnNodes(cpuUsage: CPUUsageByNode): void {
        for (const [nodeId, count] of Object.entries(cpuUsage)) {
            const node = this._nodes.get(nodeId);
            if (node) {
                node.increaseAvailableCpus(count);
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