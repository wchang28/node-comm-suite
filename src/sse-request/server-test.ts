import express, { Request, Response } from 'express';
import morgan from 'morgan';
import helmet from 'helmet';
import {ConnectionManager as RequestConnectionManager} from './connection-manager.js';
import {
    CPUs,
    NodeIdentifyInfo,
    TaskUtilizationInfo,
    NodeHealthInfo
} from '../type.js';
import { Node, NodesTracker } from '../dispatcher/nodes.js';
import {DispatchSession} from '../dispatcher/dispatch-session.js';
import {sleep} from '../utils.js';

const PORT: number = (process.env.PORT ? parseInt(process.env.PORT) : 80);
const HOSTNAME: string = process.env.BIND || '0.0.0.0';

const app = express();
app.disable('x-powered-by');
app.set('trust proxy', true);
app.use(helmet());
app.use(morgan('combined'));

app.use((req: Request, res: Response, next) => {
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Access-Control-Allow-Origin', '*');
    next();
});

// Health check endpoint
app.get('/health', (req: Request, res: Response) => {
    res.json({
        status: 'ok',
    });
});

const taskLauncherRouter = express.Router();
app.use('/task-launcher', taskLauncherRouter);

const PING_INTERVAL_MS = 30000; // 30 seconds
const REQUEST_TIMEOUT_MS = 30000; // 30 seconds
const connectionManager = new RequestConnectionManager(PING_INTERVAL_MS, REQUEST_TIMEOUT_MS);

const nodesTracker = new NodesTracker();
nodesTracker.on("node-added", (node) => {
    console.log(`Node <<added>>: ${node.hostname} (ID: ${node.id}, Session ID: ${node.sessionId}, Total CPUs: ${node.totalCpus})`);
}).on("node-removed", (node) => {
    console.log(`Node <<removed>>: ${node.hostname} (ID: ${node.id}, Session ID: ${node.sessionId})`);
}).on("change", () => {
    console.log(`NodesTracker <<changed>>. Current nodes: ${nodesTracker.numNodes}, Usable CPUs: ${nodesTracker.numUsableCpus}`);
});

connectionManager.on("connected", async (connection) => {
    const connId = connection.connId;
    console.log(`<<New Connection>> established. Connection ID: ${connId}`);
    console.log(`no. of connections = ${connectionManager.numConnections}`);
    const result = await connection.request<NodeIdentifyInfo>("/identify", "GET");
    console.log(`<<request>> (GET /identify) response:`, JSON.stringify(result));
    const {sessionId, hostname, availableCPUs, currentLoading} = result;
    const nodeId = connId;
    const node = new Node(connection, nodeId, sessionId, hostname, availableCPUs, currentLoading);
    nodesTracker.addNode(node);

    node.on("task-finished", (payload) => {
        console.log(`Node ${node.hostname} (ID: ${node.id}) <<task-finished>>:`, JSON.stringify(payload));
    });

    await sleep(100);
    const result_2 = await connection.request<TaskUtilizationInfo>("/task/utilization", "GET");
    console.log(`<<request>> (GET /task/utilization) response:`, JSON.stringify(result_2));
    await sleep(100);
    const result_3 = await connection.request<NodeHealthInfo>("/health", "GET");
    console.log(`<<request>> (GET /health) response:`, JSON.stringify(result_3));
    await sleep(100);
    const taskItems: {j: string, t: number}[] = [
        {j: "25", t: 3},
        {j: "24", t: 10},
        {j: "24", t: 22},
        {j: "23", t: 0},
        {j: "26", t: 10},
        {j: "26", t: 14}
    ];
    const usableCPUs: CPUs = new Array(6).fill(connId);
    const dispatchSession = new DispatchSession(nodesTracker);
    dispatchSession.on("cpus-usage-assignment", (cpusUsage) => {
        console.log(`DispatchSession <<cpus-usage-assignment>> event emitted. CPUs assigned to tasks: ${JSON.stringify(cpusUsage)}`);
        nodesTracker.increaseCPUUsageOnNodes(cpusUsage);
    }).on("one-task-dispatched", (row) => {
        console.log(`DispatchSession <<one-task-dispatched>> event emitted. Task ${row.j}-${row.t} dispatched to node ${row.nodeId}, pid: ${row.pid}, error: ${row.error}, duration: ${row.durationMS}ms`);
    }).on("session-completed", (sessionJSON) => {
        console.log(`DispatchSession <<session-completed>> event emitted. All tasks dispatched. session: ${JSON.stringify(sessionJSON)}`);
    });
    const errors = await dispatchSession.dispatch(usableCPUs, taskItems);
    nodesTracker.decreaseCPUUsageOnNodes(errors);

    console.log(`Client ${connId}: Number of pending requests: ${connection.numPendingRequests}`);
}).on("disconnected", (connection) => {
    console.log(`<<Disconnected>>. Connection ID: ${connection.connId}`);
    console.log(`no. of connections = ${connectionManager.numConnections}`);
    const nodeId = connection.connId;
    nodesTracker.removeNode(nodeId);
});

// SSE endpoint for task launcher: /task-launcher/events
connectionManager.setupExpressRouter(taskLauncherRouter, "/events");

// Start server
app.listen(PORT, HOSTNAME, () => {
    console.log(`SSE Server running on http://${HOSTNAME}:${PORT}`);
    console.log(`Task launcher SSE endpoint: http://${HOSTNAME}:${PORT}/task-launcher/events`);
    console.log(`Health check: http://${HOSTNAME}:${PORT}/health`);
});
