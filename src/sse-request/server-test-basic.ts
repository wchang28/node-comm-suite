import express, { Request, Response } from 'express';
import morgan from 'morgan';
import helmet from 'helmet';
import {ConnectionManager as RequestConnectionManager} from './connection-manager.js';
import {NodeIdentifyInfo} from '../type.js';

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

connectionManager.on("connected", async (connection) => {
    connection.on("message", (event: string, data?: any) => {
        console.log(`<<NOTIFICATION>> recieved from client ${connection.connId}, event: <<${event}>>, data: ${JSON.stringify(data)}`);
    });
    const connId = connection.connId;
    console.log(`<<New Connection>> established. Connection ID: ${connId}`);
    console.log(`no. of connections = ${connectionManager.numConnections}`);
    const result = await connection.request<NodeIdentifyInfo>("/identify", "GET");
    console.log(`<<request>> (GET /identify) response:`, JSON.stringify(result));
    console.log(`Client ${connId}: Number of pending requests: ${connection.numPendingRequests}`);
}).on("disconnected", (connection) => {
    console.log(`<<Disconnected>>. Connection ID: ${connection.connId}`);
    console.log(`no. of connections = ${connectionManager.numConnections}`);
});

// SSE endpoint for task launcher: /task-launcher/events
connectionManager.setupExpressRouter(taskLauncherRouter, "/events");

// Start server
app.listen(PORT, HOSTNAME, () => {
    console.log(`SSE Server running on http://${HOSTNAME}:${PORT}`);
    console.log(`Task launcher SSE endpoint: http://${HOSTNAME}:${PORT}/task-launcher/events`);
    console.log(`Health check: http://${HOSTNAME}:${PORT}/health`);
});
