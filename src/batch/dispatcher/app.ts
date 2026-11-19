import express, { NextFunction, Request, Response } from 'express';
import morgan from 'morgan';
import helmet from 'helmet';
import {ConnectionManager as RequestConnectionManager} from '../../sse-request/connection-manager.js';
import {Dispatcher} from "./dispatcher.js";
import { JobId } from "../types.js";
import { DBConfigsFile, toDBConfig } from "../../db-types.js";
import fs from 'node:fs';
import { createPoolDataPlaneDBPool } from "./db-pools.js";
import { RequestGetSet } from "./request-get-set.js";
import { router as jobRouter } from './job/index.js';
import { IDispatcherAbstraction } from './types.js';
import { ClientConnectionsMonitor } from './client-connections-monitor.js';
import { MostRecentJobsMonitor } from './most-recent-jobs-monitor.js';

const PORT: number = (process.env.PORT ? parseInt(process.env.PORT) : 8080);
const HOSTNAME: string = process.env.HOSTNAME || '0.0.0.0';

// DB settings come from a JSON file (default ./db-configs.json, override with DB_CONFIGS_PATH)
const dbConfigsPath = process.env.DB_CONFIGS_PATH ?? "db-configs.json";
const dbConfig = toDBConfig(JSON.parse(fs.readFileSync(dbConfigsPath, "utf8")) as DBConfigsFile);
console.log(`Using DB type: ${dbConfig.type}, schema: ${dbConfig.schema} (from ${dbConfigsPath})`);
const poolDataPlaneDBPool = createPoolDataPlaneDBPool(dbConfig.type, dbConfig.connectConfig, dbConfig.schema);

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


const PING_INTERVAL_MS = 30000; // 30 seconds
const REQUEST_TIMEOUT_MS = 30000; // 30 seconds

const nodesConnectionManager = new RequestConnectionManager(PING_INTERVAL_MS, REQUEST_TIMEOUT_MS);

const JOB_KILL_RETRIES = 2;
const dispatcher = new Dispatcher(nodesConnectionManager, JOB_KILL_RETRIES);

dispatcher.on("dispatching", (sessionId) => {
    console.log(`Dispatcher <<dispatching>>: sessionId=${sessionId}`);
}).on("dispatch-completed", (dispatchSession) => {
    console.log(`Dispatcher <<dispatch-completed>>:`, JSON.stringify(dispatchSession.toJSON()));
}).on("one-task-dispatched", (row) => {
    console.log(`Dispatcher <<one-task-dispatched>>:`, JSON.stringify(row));
}).on("killing-job", (jobId) => {
    console.log(`Dispatcher <<killing-job>>: jobId=${jobId}`);
}).on("job-kill-completed", (jobId) => {
    console.log(`Dispatcher <<job-kill-completed>>: jobId=${jobId}`);
}).on("job-kill-retry", (jobId, retryIndex, numProcessKilled) => {
    console.log(`Dispatcher <<job-kill-retry>>: jobId=${jobId}, retryIndex=${retryIndex}, numProcessKilled=${numProcessKilled}`);
});

const nodesTracker = dispatcher.nodesTracker;
nodesTracker.on("node-added", (node) => {
    console.log(`Node <<added>>: ${node.hostname} (ID: ${node.id}, Session ID: ${node.sessionId}, Total CPUs: ${node.totalCpus})`);
}).on("node-removed", (node) => {
    console.log(`Node <<removed>>: ${node.hostname} (ID: ${node.id}, Session ID: ${node.sessionId})`);
}).on("change", () => {
    console.log(`NodesTracker <<changed>>. Current nodes: ${nodesTracker.numNodes}, Usable CPUs: ${nodesTracker.numUsableCpus}`);
}).on("task-finished-on-node", (node, payload) => {
    console.log(`Node ${node.hostname} (ID: ${node.id}) <<task-finished-on-node>>:`, JSON.stringify(payload));
});

const tasksQueue = dispatcher.queue;

tasksQueue.on("change", () => {
    
});

const taskLauncherRouter = express.Router();
app.use('/task-launcher', taskLauncherRouter);
// SSE endpoint for task launcher: /task-launcher/events
nodesConnectionManager.setupExpressRouter(taskLauncherRouter, "/events");

const dispatcherMonitorConnectionManager = new RequestConnectionManager(PING_INTERVAL_MS, REQUEST_TIMEOUT_MS);
const recentJobsMonitorConnectionManager = new RequestConnectionManager(PING_INTERVAL_MS, REQUEST_TIMEOUT_MS);
const connectionsMonitorConnectionManager = new RequestConnectionManager(PING_INTERVAL_MS, REQUEST_TIMEOUT_MS);

const clientConnectionsMonitor = new ClientConnectionsMonitor(
    dispatcherMonitorConnectionManager,
    recentJobsMonitorConnectionManager,
    connectionsMonitorConnectionManager
);

clientConnectionsMonitor.on("change", () => {
    connectionsMonitorConnectionManager.broadcastNotification("change", clientConnectionsMonitor.toJSON());
});

const mostRecentJobsMonitor = new MostRecentJobsMonitor(poolDataPlaneDBPool, 300, 500);
mostRecentJobsMonitor
.on("change", (jobs) => {
    recentJobsMonitorConnectionManager.broadcastNotification("change", jobs);
}).start();

class LocalDispatcherAbstraction implements IDispatcherAbstraction {
    constructor(private _dispatcher: Dispatcher) {
    }
    async queueJob(jobId: JobId, numTasks: number){
        this._dispatcher.queue.queueJob(jobId, numTasks);
    }
    async killJob(jobId: JobId) {
        await this._dispatcher.killJob(jobId);
    }
}

const authorizationMiddleware = async (req: Request, res: Response, next: NextFunction) => {
    // Implement your authorization logic here
    const req_gs = new RequestGetSet(req);
    const authHeader = req.headers.authorization || "";
    function getBearerToken() {
        if (authHeader.startsWith("Bearer ")) {
            return authHeader.slice("Bearer ".length);
        }
        return null;
    }
    req_gs.user = {
        id: "00000000-0000-0000-0000-000000000001",
        name: "Test Person"
    };
    next();
};

app.use(authorizationMiddleware, (req: Request, res: Response, next: NextFunction) => {
    const req_gs = new RequestGetSet(req);
    req_gs.poolDataPlaneDBPool = poolDataPlaneDBPool;
    req_gs.dispatcher = new LocalDispatcherAbstraction(dispatcher);
    req_gs.fetchSelf = (path: string, init?: RequestInit) => {
        if (req.headers.authorization) {
            if (!init) init = {};
            if (!init.headers) init.headers = {};
            (init.headers as Record<string, string>)['Authorization'] = req.headers.authorization;
        }
        return fetchSelf(path, init);
    };
    next();
});

app.get("/me", (req: Request, res: Response) => {
    const req_gs = new RequestGetSet(req);
    res.json(req_gs.user);
});

const dispatcherMonitorRouter = express.Router();
app.use('/dispatcher-monitor', dispatcherMonitorRouter);
// SSE endpoint for dispatcher monitor: /dispatcher-monitor/events
dispatcherMonitorConnectionManager.setupExpressRouter(dispatcherMonitorRouter, "/events");

const recentJobsMonitorRouter = express.Router();
app.use('/recent-jobs-monitor', recentJobsMonitorRouter);
// SSE endpoint for recent jobs monitor: /recent-jobs-monitor/events
recentJobsMonitorConnectionManager.setupExpressRouter(recentJobsMonitorRouter, "/events");

const connectionsMonitorRouter = express.Router();
app.use('/connections-monitor', connectionsMonitorRouter);
// SSE endpoint for connections monitor: /connections-monitor/events
connectionsMonitorConnectionManager.setupExpressRouter(connectionsMonitorRouter, "/events");

app.get("/client-connections", (req: Request, res: Response) => {
    res.json(clientConnectionsMonitor.toJSON());
});

app.use('/job', jobRouter);

// Start server
const server = app.listen(PORT, HOSTNAME, () => {
    console.log(`SSE Server running on http://${HOSTNAME}:${PORT}`);
    console.log(`Task launcher SSE endpoint: http://${HOSTNAME}:${PORT}/task-launcher/events`);
    console.log(`Dispatcher monitor SSE endpoint: http://${HOSTNAME}:${PORT}/dispatcher-monitor/events`);
    console.log(`Recent jobs monitor SSE endpoint: http://${HOSTNAME}:${PORT}/recent-jobs-monitor/events`);
    console.log(`Connections monitor SSE endpoint: http://${HOSTNAME}:${PORT}/connections-monitor/events`);
    console.log(`Health check: http://${HOSTNAME}:${PORT}/health`);
});

// Disable all built-in Node.js HTTP timeouts
server.timeout = 0;
server.requestTimeout = 0;
server.headersTimeout = 0;

/**
 * Send an HTTP request to this express app itself.
 * @param path request path, e.g. "/health" or "/job/23"
 * @param init standard fetch options (method, headers, body, signal, ...)
 */
async function fetchSelf(path: string, init?: RequestInit): Promise<globalThis.Response> {
    if (!server.listening) {
        await new Promise<void>((resolve, reject) => {
            server.once('listening', resolve);
            server.once('error', reject);
        });
    }
    const addr = server.address();
    if (!addr || typeof addr === 'string') {
        throw new Error(`Unsupported server address: ${addr}`);
    }
    // a wildcard bind address (0.0.0.0 / ::) isn't a valid target; use loopback instead
    const host = (addr.address === '0.0.0.0' || addr.address === '::')
        ? '127.0.0.1'
        : (addr.family === 'IPv6' ? `[${addr.address}]` : addr.address);
    const url = new URL(path, `http://${host}:${addr.port}`);
    return fetch(url, init);
}

/*
// example usage
fetchSelf('/health')
    .then((res) => res.json())
    .then((body) => console.log(`fetchSelf /health:`, JSON.stringify(body)))
    .catch((err) => console.error(`fetchSelf /health failed:`, err));
*/

for (const sig of ['SIGTERM', 'SIGINT'] as const) {
    process.once(sig, () => {
        console.log(`${sig} received, shutting down...`);
        const serverClosedPromise = new Promise<void>((resolve) => {
            server.close(() => resolve());
        });
        const mostRecentJobsMonitorStoppedPromise = mostRecentJobsMonitor.stop();
        const promises = Promise.all([serverClosedPromise, mostRecentJobsMonitorStoppedPromise]);
        promises.then(() => {
            console.log(`Shutdown complete.`);
            process.exit(0);
        });
        setTimeout(() => process.exit(1), 10_000).unref();
    });
}