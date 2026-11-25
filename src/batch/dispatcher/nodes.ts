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
    NodesTrackerJSON,
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
    "idle": () => void; // fired when the node becomes idle (running tasks -> not running tasks)
    "active": () => void; // fired when the node becomes active (not running tasks -> running tasks)
    "task-slots-change": () => void; // fire when any (available/usable) task slots related change
    "task-finished": (payload: TaskFinishedMsgPayload) => void; // fired when a task has finished on this node
}

export class Node extends EventEmitter {
    private _enabled: boolean = true;
    private _numAvailableTaskSlots: number;
    private _dismounting: boolean = false;
    private _lastIdleTime: number | null;
    constructor(private _connection: Connection, public id: NodeId, public sessionId: NodeSessionId, public hostname: NodeName, public totalTaskSlots: number, private _currentLoading: SystemLoadingInfo) {
        super();
        assert(this.id, "Node ID must be defined");
        assert(this.id === this._connection.connId, `Node ID ${this.id} must match connection ID ${this._connection.connId}`);
        assert(this.sessionId, `Node session ID musst be defined (node=${id})`);
        assert(this.hostname, `Node hostname must be defined (node=${id})`);
        assert(this.totalTaskSlots > 0, `Total CPUs must be greater than 0 (node=${id}, totalTaskSlots=${totalTaskSlots})`);
        this._numAvailableTaskSlots = this.totalTaskSlots;
        this._dismounting = false;
        this._lastIdleTime = Date.now();
        this._connection.on("message", (event: string, data?: any) => { // the connection received a message
            if (event === "task-finished") {    // one task is finished on this node
                this.increaseAvailableTaskSlots(1);   // increase the number of available task slots by 1
                this.safeEmit('task-finished', data as TaskFinishedMsgPayload);
            }
        });
    }
    get numAvailableTaskSlots(): number {
        return this._numAvailableTaskSlots;
    }
    get numUsedTaskSlots(): number {
        return this.totalTaskSlots - this._numAvailableTaskSlots;
    }
    get idle(): boolean {
        return (this.numUsedTaskSlots === 0);
    }
    get lastIdleTime(): number | null {
        return this._lastIdleTime;
    }
    private set lastIdleTime(time: number | null) {
        const oldLastIdleTime = this._lastIdleTime;
        if (oldLastIdleTime !== time) {
            this._lastIdleTime = time;
            this.safeEmit('change');
        }
    }
    get dismounting(): boolean {
        return this._dismounting;
    }
    private set dismounting(value: boolean) {
        const oldValue = this._dismounting;
        if (oldValue !== value) {
            this._dismounting = value;
            this.safeEmit('change');
        }
    }
    get currentLoading(): SystemLoadingInfo {
        return this._currentLoading;
    }
    // update the current loading info of this node, and emit 'change' event if the loading info has changed
    private set currentLoading(loading: SystemLoadingInfo) {
        const oldLoadingJSON = JSON.stringify(this._currentLoading);
        const newLoadingJSON = JSON.stringify(loading);
        this._currentLoading = loading;
        if (oldLoadingJSON !== newLoadingJSON) {
            this.safeEmit('change');
        }
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
        const wasIdle = this.idle;
        this._numAvailableTaskSlots -= count;
        if (this._numAvailableTaskSlots < 0) {
            this._numAvailableTaskSlots = 0;
        }
        if (this._numAvailableTaskSlots !== old) {
            this.safeEmit('task-slots-change');
            this.safeEmit('change');
        }
        if (wasIdle && !this.idle) {    // idle -> not idle
            this.lastIdleTime = null;
            this.safeEmit('active');
        }
    }
    increaseAvailableTaskSlots(count: number): void {
        assert(count > 0, "Task slot increase count must be positive");
        const old = this._numAvailableTaskSlots;
        const wasIdle = this.idle;
        this._numAvailableTaskSlots += count;
        if (this._numAvailableTaskSlots > this.totalTaskSlots) {
            this._numAvailableTaskSlots = this.totalTaskSlots;
        }
        if (this._numAvailableTaskSlots !== old) {
            this.safeEmit('task-slots-change');
            this.safeEmit('change');
        }
        if (!wasIdle && this.idle) {    // not idle -> idle
            this.lastIdleTime = Date.now();
            this.safeEmit('idle');
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
            this.safeEmit('task-slots-change');
            this.safeEmit('change');
        }
    }
    enable() {
        this.enabled = true;
    }
    disable() {
        this.enabled = false;
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
    // make a request to the node
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
    async updateSystemLoading() {
        try {
            const loading = await this.request<SystemLoadingInfo>("/system-loading", "GET");
            this.currentLoading = loading;
        } catch (_) {}
    }
    // wait for node to become idle
    async forIdle() {
        let idlePrmmise: Promise<void> = Promise.resolve();
        if (!this.idle) {
            idlePrmmise = new Promise<void>((resolve) => {
                this.once("idle", () => resolve());
            });
        }
        await idlePrmmise;
    }
    // return whether the node is a candidate for dismounting
    get dismountableCandidate() : boolean {
        return (this._enabled && this.idle && !this.dismounting);
    }
    // prepare the node for dismounting by disabling it and waiting for it to become idle
    // returns the node ID if successfully prepared for dismounting, otherwise returns null
    async prepareForDismounting() {
        try {
            this.disable(); // disable the node for first before dismounting
            // mark the node as dismounting if it is not already marked
            if (!this.dismounting) {
                this.dismounting = true;
            }
            await this.forIdle();   // wait for the node to become idle before dismounting
            return this.id; // if successful, return the node ID
        } catch(_) {
            return null;    // if failed, return null
        }
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
            idle: this.idle,
            lastIdleTime: this.lastIdleTime,
            dismounting: this.dismounting,
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
    "task-slots-change": () => void; // fired when any (available/usable) task slots related change
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
        }).on('task-slots-change', () => {
            this.safeEmit('task-slots-change');
        }).on('task-finished', (payload) => {
            this.safeEmit('task-finished-on-node', node, payload);
        });
        this.safeEmit('node-added', node);
        this.safeEmit('task-slots-change');
        this.safeEmit('change');
        const newNumNodes = this._nodes.size;
        if (oldNumNodes == 0 && newNumNodes > 0) {  // the first node has been added
            // The first node has been added
            this.safeEmit('nodes-present');
        }
    }
    // get a node by its ID, throw error if not found
    getNode(nodeId: NodeId): Node {
        const node = this._nodes.get(nodeId)
        if (!node) {
            const message: ConnectionManagerError = "bad-connection-id";
            const cause = `Node with ID ${nodeId} not found`;
            throw new Error(message, {cause});
        }
        return node;
    }
    // get a node by its ID, return undefined if not found
    node(nodeId: NodeId): Node | undefined {
        return this._nodes.get(nodeId);
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
            this.safeEmit('task-slots-change');
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
    // enable or disable multiple nodes by their IDs, or all nodes if nodeIds is null or empty
    setNodesEnabled(nodeIds: NodeId[] | null, enabled: boolean): void {
        if (nodeIds != null && nodeIds.length > 0) {
            for (const nodeId of nodeIds) {
                const node = this._nodes.get(nodeId);
                if (node) {
                    node.enabled = enabled;
                }
            }
        } else {
            for (const node of this._nodes.values()) {
                node.enabled = enabled;
            }
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
    // Get the total number of available task slots across all nodes
    get numAvailableTaskSlots(): number {
        let total: number = 0;
        for (const node of this._nodes.values()) {
            total += node.numAvailableTaskSlots;
        }
        return total;
    }
    // Get the total number of used task slots across all nodes
    get numUsedTaskSlots(): number {
        let total: number = 0;
        for (const node of this._nodes.values()) {
            total += node.numUsedTaskSlots;
        }
        return total;
    }
    get totalTaskSlots(): number {
        let total: number = 0;
        for (const node of this._nodes.values()) {
            total += node.totalTaskSlots;
        }
        return total;
    }
    get utilizationPercent(): number | null {
        const total = this.totalTaskSlots;
        const used = this.numUsedTaskSlots;
        if (total > 0 && used >= 0) {
            return (used / total) * 100.0;
        } else {
            return null;
        }
    }
    get utilization(): string {
        const utilizationPercent = this.utilizationPercent;
        const percent = (utilizationPercent != null ? `${utilizationPercent.toFixed(2)}%` : 'N/A');
        const s =`${this.numUsedTaskSlots}/${this.totalTaskSlots}=${percent}`;
        return s;
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
    // get the current health information for a specific node
    async getNodeHealth(nodeId: NodeId) {
        return await this.getNode(nodeId).getHealth();
    }
    // updated system loading information for all nodes
    async updateNodesSystemLoading() {
        const nodes = Array.from(this._nodes.values());
        const promises = nodes.map(node => node.updateSystemLoading());
        await Promise.all(promises);
    }
    // return the list of dismountable candidate nodes at the moment
    get dismountableCandidateNodes(): NodeId[] {
        const candidateNodes = Array.from(this._nodes.values()).filter(node => (node.dismountableCandidate));
        return candidateNodes.map(node => node.id);
    }
    // prepare the candidate nodes for dismounting
    // returns the list of candidate node IDs that were successfully prepared for dismounting
    async prepareNodesForDismounting(candidateNodeIds: NodeId[]) {
        const promises: Promise<NodeId | null>[] = [];
        for (const nodeId of candidateNodeIds) {
            const node = this.node(nodeId);
            if (node && node.dismountableCandidate) {
                promises.push(node.prepareForDismounting());
            }
        }
        const results = await Promise.all(promises);
        const preparedForDismountingNodeIds = results.filter(result => result !== null) as NodeId[];
        return preparedForDismountingNodeIds;
    }
    get nodesSessionIds(): NodeSessionId[] {
        return this._nodeList.map(node => node.sessionId);
    }
    toJSON(): NodesTrackerJSON {
        const nodes = this._nodeList.map(node => node.toJSON());
        return {
            numNodes: this.numNodes,
            numAvailableTaskSlots: this.numAvailableTaskSlots,
            numUsedTaskSlots: this.numUsedTaskSlots,
            totalTaskSlots: this.totalTaskSlots,
            numUsableTaskSlots: this.numUsableTaskSlots,
            utilizationPercent: this.utilizationPercent,
            utilization: this.utilization,
            nodes
        };
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