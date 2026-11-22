import { EventEmitter } from 'node:events';
import {
    NodeId,
    NodeName,
    NodeSessionId,
    SystemLoadStatus,
    SystemLoadingInfo,
    TaskSlots,
    TaskSlotsUsageByNode,
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
    "change": () => void;
    "enabled": () => void;
    "disabled": () => void;
    "task-finished": (payload: TaskFinishedMsgPayload) => void; // fired when a task has finished on this node
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
        this._connection.on("message", (event: string, data?: any) => { // the connection received a message
            if (event === "task-finished") {    // one task is finished on this node
                me.increaseAvailableTaskSlots(1);   // increase the number of available task slots by 1
                me.safeEmit('task-finished', data as TaskFinishedMsgPayload);
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
        this.safeEmit('change');
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
            this.safeEmit('change');
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
            this.safeEmit('change');
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
            if (this._enabled) {
                this.safeEmit('enabled');
            } else {
                this.safeEmit('disabled');
            }
            this.safeEmit('change');
        }
    }
    get totalEnabledTaskSlots(): number {
        return this.enabled ? this.totalTaskSlots : 0;
    }
    get numUsableTaskSlots(): number {
        return this.enabled ? this._numAvailableTaskSlots : 0;
    }
    get usableTaskSlots(): TaskSlots {
        const taskSlots: TaskSlots = [];
        if (this.enabled) {
            for (let i = 0; i < this._numAvailableTaskSlots; i++) {
                taskSlots.push(this.id);
            }
        }
        return taskSlots;
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
        listener: NodeEvents[K]
    ): this {
        return super.on(event, listener);
    }
    off<K extends keyof NodeEvents>(
        event: K,
        listener: NodeEvents[K]
    ): this {
        return super.off(event, listener);
    }
    emit<K extends keyof NodeEvents>(
        event: K,
        ...args: Parameters<NodeEvents[K]>
    ): boolean {
        return super.emit(event, ...args);
    }
    once<K extends keyof NodeEvents>(
        event: K,
        listener: NodeEvents[K]
    ): this {
        return super.once(event, listener);
    }
    safeEmit<U extends keyof NodeEvents>(
        event: U, ...args: Parameters<NodeEvents[U]>
    ): void {
        try {
            this.emit(event, ...args);
        } catch (err) {
            console.error(`Node "${event}" listener threw:`, err);
        }
    }
}

export interface NodeTrackerEvents {
    "change": () => void;   // fired when any change occurs in the nodes tracker
    "node-added": (node: Node) => void;   // fired when a node is added
    "node-removed": (node: Node) => void;   // fired when a node is removed
    "nodes-present": () => void;    // fired when nodes are present (from empty to non-empty)
    "nodes-empty": () => void;  // fired when no nodes are present (from non-empty to empty)
    "task-finished-on-node": (node: Node, payload: TaskFinishedMsgPayload) => void; // fired when a task finishes on a node
}

export class NodesTracker extends EventEmitter implements INodeTaskDispatcher {
    private _nodes: Map<NodeId, Node> = new Map<NodeId, Node>();
    private _nodeList: Node[] = [];
    constructor() {
        super();
    }
    // add a new node to the tracker
    addNode(node: Node): void {
        const oldNumNodes = this._nodes.size;
        this._nodes.set(node.id, node);
        this._nodeList.push(node);
        node.on('change', () => {
            this.safeEmit('change');
        }).on('task-finished', (payload) => {
            this.safeEmit('task-finished-on-node', node, payload);
        });
        this.safeEmit('node-added', node);
        this.safeEmit('change');
        const newNumNodes = this._nodes.size;
        if (oldNumNodes == 0 && newNumNodes > 0) {  // the first node has been added
            // The first node has been added
            this.safeEmit('nodes-present');
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
    // remove a node from the tracker by its ID
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
            this.safeEmit('node-removed', node);
            this.safeEmit('change'); 
        }
        const newNumNodes = this._nodes.size;
        if (oldNumNodes > 0 && newNumNodes === 0) {
            this.safeEmit('nodes-empty');
        }
    }
    // enable or disable a node by its ID
    setNodeEnabled(nodeId: NodeId, enabled: boolean): void {
        const node = this._nodes.get(nodeId);
        if (node) {
            node.enabled = enabled;
        }
    }
    isNodeEnabled(nodeId: NodeId): boolean {
        const node = this._nodes.get(nodeId);
        if (node) {
            return node.enabled;
        } else {
            throw new Error(`Node with ID ${nodeId} not found`);
        }
    }
    get numNodes(): number {
        return this._nodes.size;
    }
    // Get the total number of usable task slots across all nodes
    get numUsableTaskSlots(): number {
        let total: number = 0;
        for (const node of this._nodes.values()) {
            total += node.numUsableTaskSlots;
        }
        return total;
    }
    // Returns whether there are any usable task slots available across all nodes
    get hasUsableTaskSlots(): boolean {
        return (this.numUsableTaskSlots > 0);
    }
    // Get the list of all usable task slots across all nodes
    get usableTaskSlots(): TaskSlots {
        const taskSlots: TaskSlots = [];
        for (const node of this._nodes.values()) {  // for each node
            taskSlots.push(...node.usableTaskSlots);
        }
        return taskSlots;
    }
    // call this function to reflect the task slots usage on nodes after dispatching tasks
    increaseTaskSlotsUsageOnNodes(taskSlotsUsage: TaskSlotsUsageByNode): void {
        for (const [nodeId, count] of Object.entries(taskSlotsUsage)) {
            const node = this._nodes.get(nodeId);
            if (node) {
                node.decreaseAvailableTaskSlots(count);
            }
        }
    }
    // call this function to reflect the task slots usage on nodes after failing to dispatch tasks
    decreaseTaskSlotsUsageOnNodes(taskSlotsUsage: TaskSlotsUsageByNode): void {
        for (const [nodeId, count] of Object.entries(taskSlotsUsage)) {
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
        listener: NodeTrackerEvents[K]
    ): this {
        return super.on(event, listener);
    }
    off<K extends keyof NodeTrackerEvents>(
        event: K,
        listener: NodeTrackerEvents[K]
    ): this {
        return super.off(event, listener);
    }
    emit<K extends keyof NodeTrackerEvents>(
        event: K,
        ...args: Parameters<NodeTrackerEvents[K]>
    ): boolean {
        return super.emit(event, ...args);
    }
    once<K extends keyof NodeTrackerEvents>(
        event: K,
        listener: NodeTrackerEvents[K]
    ): this {
        return super.once(event, listener);
    }
    safeEmit<K extends keyof NodeTrackerEvents>(
        event: K,
        ...args: Parameters<NodeTrackerEvents[K]>
    ): void {
        try {
            super.emit(event, ...args);
        } catch (error) {
            console.error(`NodesTracker "${event}" listener threw:`, error);
        }
    }
}