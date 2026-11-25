import express, { NextFunction, Request, Response } from 'express';
import morgan from 'morgan';
import helmet from 'helmet';
import {Connection, ConnectionManager as RequestConnectionManager} from '../sse-request/connection-manager.js';
import {is_json_content} from '../utils.js';

const PORT: number = (process.env.PORT ? parseInt(process.env.PORT) : 8080);
const LISTEN_HOST: string = process.env.LISTEN_HOST || '0.0.0.0';

const app = express();
app.disable('x-powered-by');
app.set('trust proxy', true);
app.use(helmet());
app.use(morgan('combined'));

app.use((req: Request, res: Response, next) => {
    res.setHeader('Cache-Control', 'no-cache, no-transform');
    res.setHeader('Access-Control-Allow-Origin', '*');
    next();
});

app.get('/', (req: Request, res: Response) => {
    res.status(200).json({message: "ok"});
});

const agentRouter = express.Router();
app.use('/agent', agentRouter);

const PING_INTERVAL_MS = 30000; // 30 seconds
const REQUEST_TIMEOUT_MS = 1800000; // 1800000 seconds = 30 mins
const connectionManager = new RequestConnectionManager(PING_INTERVAL_MS, REQUEST_TIMEOUT_MS, 1);

connectionManager.on("connected", async (connection) => {
    console.log(`client <<connected>>. Connection ID: ${connection.connId}`);
    console.log(`# of connections = ${connectionManager.numConnections}`);
}).on("disconnected", (connection) => {
    console.log(`client <<disconnected>>. Connection ID: ${connection.connId}`);
    console.log(`# of connections = ${connectionManager.numConnections}`);
}).on("connection-rejected", (remoteAddress, remotePort) => {
    console.log(`client <<connection-rejected>>. remoteAddress=${remoteAddress}, remotePort=${remotePort}`);
});

// SSE endpoint for agent: /agent/events
connectionManager.setupExpressRouter(agentRouter, "/events");

const agentConnectedCheckMiddleware = (req: Request, res: Response, next: NextFunction) => {
    const conns = connectionManager.connections;
    if (conns.length == 0) {    // no agent client connection at this moment
        // wait for 3 seconds to see if an agent client connection becomes available
        const timer = setTimeout(() => {
            // 3 seconds have passed, check if an agent client is connected
            clearTimeout(timer);    // clear the timeout
            if (connectionManager.connections.length == 0) {    // still no agent client connection
                res.status(503).json({status: "on-premises agent not connected"});
            } else {    // an agent client is now connected
                ((req as any) as {agentConnection: Connection}).agentConnection = connectionManager.connections[0];
                next();
            }
        }, 3000); // 3 seconds
    } else {    // an agent client is connected
        ((req as any) as {agentConnection: Connection}).agentConnection = conns[0];
        next();
    }
};

app.use(
agentConnectedCheckMiddleware,
express.json({limit: '999mb'}),
express.text({
    limit: '999mb',
    type: ['text/plain', 'text/csv', 'text/tab-separated-values'] 
}),
async (req: Request, res: Response) => {
    const {agentConnection} = ((req as any) as {agentConnection: Connection});
    const method = req.method;
    const headers:{[key: string]: string} = (req.headers as any);
    let body: any = req.body;   // string | object/null | undefined
    //console.log(`typeof body=${typeof body}, body=${body}`);
    const query: {[key: string]:string} = {};
    for (const [key, value] of Object.entries(req.query)) {
        query[key] = value as string;
    }
    const queryString = new URLSearchParams(query).toString();
    //console.log(`typeof queryString=${typeof queryString}, queryString=${queryString}`);
    const path = (queryString.length > 0 ? `${req.path}?${queryString}` : req.path);
    const response = await agentConnection.request_raw(path, method, body, headers);
    for (const key in response.headers) {
        res.setHeader(key, response.headers[key]);
    }
    const hasContent = ("content-type" in response.headers && response.headers["content-type"].length > 0);
    if (hasContent) {
        const is_json = is_json_content(response.headers["content-type"]);
        const responseBody = (is_json ? JSON.stringify(response.body) : response.body as string); 
        res.status(response.status).end(responseBody);
    } else {
        res.status(response.status).end();
    }
}
);

// Start server
const server = app.listen(PORT, LISTEN_HOST, () => {
    console.log(`Proxy server running on http://${LISTEN_HOST}:${PORT}`);
    console.log(`Agent connection SSE endpoint: http://${LISTEN_HOST}:${PORT}/agent/events`);
    console.log(`Health check: http://${LISTEN_HOST}:${PORT}/health`);
});

// Disable all built-in Node.js HTTP timeouts
server.timeout = 0;
server.requestTimeout = 0;
server.headersTimeout = 0;
