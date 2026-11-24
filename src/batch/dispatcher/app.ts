import express, { NextFunction, Request, Response } from 'express';
import morgan from 'morgan';
import helmet from 'helmet';
import {ConnectionManager as RequestConnectionManager} from '../../sse-request/connection-manager.js';
import {Dispatcher} from "./dispatcher.js";
import { JobId, NodeId } from "../types.js";
import { DBConfigsFile, toDBConfig } from "../../db-types.js";
import fs from 'node:fs';
import { createPoolDataPlaneDBPool } from "./db-pools.js";
import { RequestGetSet } from "./request-get-set.js";
import { router as dispatcherRouter } from './dispatcher/index.js';
import { router as jobRouter } from './job/index.js';
import { IDispatcherAbstraction } from './types.js';
import { DispatchMonitor } from './dispatch-monitor.js';
import { MostRecentJobsMonitor } from './most-recent-jobs-monitor.js';
import { ClientConnectionsMonitor } from './client-connections-monitor.js';

const PORT: number = (process.env.PORT ? parseInt(process.env.PORT) : 8080);
const LISTEN_HOST: string = process.env.LISTEN_HOST || '0.0.0.0';

// DB settings come from a JSON file (default ./db-configs.json, override with DB_CONFIGS_PATH)
const dbConfigsPath = process.env.DB_CONFIGS_PATH ?? "db-configs.json";
const dbConfig = toDBConfig(JSON.parse(fs.readFileSync(dbConfigsPath, "utf8")) as DBConfigsFile);
console.log(`Using DB type: ${dbConfig.type}, schema: ${dbConfig.schema} (from ${dbConfigsPath})`);
const poolDataPlaneDBPool = createPoolDataPlaneDBPool(dbConfig.type, dbConfig.connectConfig, dbConfig.schema);

const DEFAULT_MOST_RECENT_JOBS_LIMIT: number = 300;

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

const PING_INTERVAL_MS: number = 30000; // 30 seconds
const REQUEST_TIMEOUT_MS: number = 30000; // 30 seconds

const nodesConnectionManager = new RequestConnectionManager(PING_INTERVAL_MS, REQUEST_TIMEOUT_MS);

const JOB_KILL_RETRIES = 2;
const dispatcher = new Dispatcher(nodesConnectionManager, JOB_KILL_RETRIES);

dispatcher.on("dispatching", (sessionId) => {
    console.log(`Dispatcher <<dispatching>>: sessionId=${sessionId}`);
}).on("dispatch-completed", (dispatchSession) => {
    console.log(`Dispatcher <<dispatch-completed>>:`, JSON.stringify(dispatchSession.toJSON()));
}).on("one-task-dispatched", (row) => {
    console.log(`Dispatcher <<one-task-dispatched>>:`, JSON.stringify(row));
}).on("enabled", () => {
    console.log(`Dispatcher <<enabled>>`);
}).on("disabled", () => {
    console.log(`Dispatcher <<disabled>>`);
}).on("started", () => {
    console.log(`Dispatcher <<started>>`);
}).on("stopping", () => {
    console.log(`Dispatcher <<stopping>>`);
}).on("stopped", () => {
    console.log(`Dispatcher <<stopped>>`);
}).on("killing-job", (jobId) => {
    console.log(`<<killing-job>>: jobId=${jobId}`);
}).on("job-killed", (jobId) => {
    console.log(`<<job-killed>>: jobId=${jobId}`);
}).on("job-kill-retry", (jobId, retryIndex, numProcessKilled) => {
    console.log(`<<job-kill-retry>>: jobId=${jobId}, retryIndex=${retryIndex}, numProcessKilled=${numProcessKilled}`);
}).on("job-kill-error", (jobId, error) => {
    console.log(`<<job-kill-error>>: jobId=${jobId}, error=${error}`);
}).start();

const nodesTracker = dispatcher.nodesTracker;
nodesTracker.on("node-added", (node) => {
    console.log(`Node <<added>>: ${node.hostname} (ID: ${node.id}, Session ID: ${node.sessionId}, Total Task Slots: ${node.totalTaskSlots})`);
}).on("node-removed", (node) => {
    console.log(`Node <<removed>>: ${node.hostname} (ID: ${node.id}, Session ID: ${node.sessionId})`);
}).on("change", () => {
    console.log(`NodesTracker <<changed>>. Current nodes: ${nodesTracker.numNodes}, Usable Task Slots: ${nodesTracker.numUsableTaskSlots}`);
}).on("task-finished-on-node", (node, payload) => {
    console.log(`Node ${node.hostname} (ID: ${node.id}) <<task-finished-on-node>>:`, JSON.stringify(payload));
});

const taskLauncherRouter = express.Router();
app.use('/task-launcher', taskLauncherRouter);
// SSE endpoint for task launcher: /task-launcher/events
nodesConnectionManager.setupExpressRouter(taskLauncherRouter, "/events");

const dispatcherEventsConnector = new RequestConnectionManager(PING_INTERVAL_MS, REQUEST_TIMEOUT_MS);
const recentJobsEventsConnector = new RequestConnectionManager(PING_INTERVAL_MS, REQUEST_TIMEOUT_MS);
const connectionsEventsConnector = new RequestConnectionManager(PING_INTERVAL_MS, REQUEST_TIMEOUT_MS);

const clientConnectionsMonitor = new ClientConnectionsMonitor(
    dispatcherEventsConnector,
    recentJobsEventsConnector,
    connectionsEventsConnector
);

clientConnectionsMonitor.on("change", () => {
    connectionsEventsConnector.broadcastNotification("change", clientConnectionsMonitor.toJSON());
});

const dispatchMonitor = new DispatchMonitor(dispatcher, 1000);
dispatchMonitor.on("queue-change", (data) => {
    dispatcherEventsConnector.broadcastNotification("queue-change", data);
}).on("nodes-change", (data) => {
    dispatcherEventsConnector.broadcastNotification("nodes-change", data);
}).on("job-killer-change", (data) => {
    dispatcherEventsConnector.broadcastNotification("job-killer-change", data);
}).on("dispatcher-change", (data) => {
    dispatcherEventsConnector.broadcastNotification("dispatcher-change", data);
}).on("started", () => {
    console.log("DispatchMonitor <<started>>");
}).on("stopping", () => {
    console.log("DispatchMonitor <<stopping>>");
}).on("stopped", () => {
    console.log("DispatchMonitor <<stopped>>");
}).start();

const mostRecentJobsMonitor = new MostRecentJobsMonitor(poolDataPlaneDBPool, DEFAULT_MOST_RECENT_JOBS_LIMIT, 500);
mostRecentJobsMonitor
.on("change", () => {
    recentJobsEventsConnector.broadcastNotification("change", mostRecentJobsMonitor.toJSON());
}).on("started", () => {
    console.log("MostRecentJobsMonitor <<started>>");
}).on("stopping", () => {
    console.log("MostRecentJobsMonitor <<stopping>>");
}).on("stopped", () => {
    console.log("MostRecentJobsMonitor <<stopped>>");
}).start();

class LocalDispatcherAbstraction implements IDispatcherAbstraction {
    constructor(private _dispatcher: Dispatcher) {
    }
    async enable() {
        this._dispatcher.enabled = true;
        return this._dispatcher.enabled;
    }
    async disable() {
        this._dispatcher.enabled = false;
        return this._dispatcher.enabled;
    }
    async isEnabled() {
        return this._dispatcher.enabled;
    }
    async closeQueue() {
        this._dispatcher.closeQueue();
        return this._dispatcher.queueClosed;
    }
    async openQueue() {
        this._dispatcher.openQueue();
        return this._dispatcher.queueClosed;
    }
    async isQueueClosed() {
        return this._dispatcher.queueClosed;
    }
    async queueJSON() {
        return this._dispatcher.queueJSON;
    }
    async nodesTrackerJSON() {
        return this._dispatcher.nodesTracker.toJSON();
    }
    async nodeJSON(nodeId: NodeId) {
        const node = this._dispatcher.nodesTracker.node(nodeId);
        if (!node) {
            return null;
        } else {
            return node.toJSON();
        }
    }
    async setNodesEnabled(nodeIds: NodeId[] | null, enabled: boolean) {
        this._dispatcher.nodesTracker.setNodesEnabled(nodeIds, enabled);
    }
    async queueJob(jobId: JobId, numTasks: number){
        this._dispatcher.queueJob(jobId, numTasks);
    }
    async killJob(jobId: JobId) {
        await this._dispatcher.killJob(jobId);
    }
    async toJSON(): Promise<any> {
        return this._dispatcher.toJSON();
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
    req_gs.defaultMostRecentJobsLimit = DEFAULT_MOST_RECENT_JOBS_LIMIT;
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
    const {user} = new RequestGetSet(req);
    res.json(user);
});

app.use('/dispatcher', dispatcherRouter);
// SSE endpoint for dispatcher monitor: /dispatcher/events
dispatcherEventsConnector.setupExpressRouter(dispatcherRouter, "/events");

const recentJobsRouter = express.Router();
app.use('/recent-jobs', recentJobsRouter);
recentJobsRouter.get("/", (req: Request, res: Response) => {
    res.json(mostRecentJobsMonitor.toJSON());
});
// SSE endpoint for recent jobs monitor: /recent-jobs/events
recentJobsEventsConnector.setupExpressRouter(recentJobsRouter, "/events");

const clientConnectionsRouter = express.Router();
app.use('/client-connections', clientConnectionsRouter);
clientConnectionsRouter.get("/", (req: Request, res: Response) => {
    res.json(clientConnectionsMonitor.toJSON());
});
// SSE endpoint for connections monitor: /client-connections/events
connectionsEventsConnector.setupExpressRouter(clientConnectionsRouter, "/events");

app.use('/job', jobRouter);

// Start server
const server = app.listen(PORT, LISTEN_HOST, () => {
    console.log(`Server running on http://${LISTEN_HOST}:${PORT}`);
    console.log(`Task launcher SSE endpoint: http://${LISTEN_HOST}:${PORT}/task-launcher/events`);
    console.log(`Dispatcher monitor SSE endpoint: http://${LISTEN_HOST}:${PORT}/dispatcher/events`);
    console.log(`Recent jobs monitor SSE endpoint: http://${LISTEN_HOST}:${PORT}/recent-jobs/events`);
    console.log(`Client connections monitor SSE endpoint: http://${LISTEN_HOST}:${PORT}/client-connections/events`);
    console.log(`Health check: http://${LISTEN_HOST}:${PORT}/health`);
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
            server.closeAllConnections();
            server.close(() => {
                console.log(`app server <<closed>>`);
                resolve();
            });
        });
        const promises = Promise.all([
            serverClosedPromise,
            dispatcher.stop(),
            dispatchMonitor.stop(),
            mostRecentJobsMonitor.stop()
        ]);
        promises.then(() => {
            console.log(`Shutdown complete.`);
            process.exit(0);
        });
        setTimeout(() => process.exit(1), 10_000).unref();
    });
}