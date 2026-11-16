import express, { Request, Response } from 'express';
import morgan from 'morgan';
import helmet from 'helmet';

const PORT: number = (process.env.PORT ? parseInt(process.env.PORT) : 80);
const HOSTNAME: string = process.env.BIND || '0.0.0.0';

const app = express();
app.disable('x-powered-by');
app.set('trust proxy', true);
app.use(helmet());
app.use(morgan('combined'));

// Store active SSE connections with their event filters
interface ClientConnection {
    remoteAddress: string;
    remotePort: number;
    response: Response;
    eventFilter: string[];
}

const clients = new Set<ClientConnection>();

// SSE endpoint
app.get('/events', (req: Request, res: Response) => {
    // Set headers for SSE
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('Access-Control-Allow-Origin', '*');
    
    // Parse event filter from query parameter
    // Example: /events?filter=random or /events?filter=random,message
    const filterParam = req.query.filter as string;
    const eventFilter = filterParam ? filterParam.split(',').map(e => e.trim()) : [];
    
    // Send initial connection message
    res.write('data: Connected to SSE server\n\n');

  
    // Add client to active connections
    const client: ClientConnection = {
        remoteAddress: req.socket.remoteAddress || '',
        remotePort: req.socket.remotePort || 0,
        response: res,
        eventFilter
    };
    clients.add(client);
    
    console.log(`Client connected with filter: [${eventFilter.length > 0 ? eventFilter.join(', ') : 'all events'}]. Total clients: ${clients.size}`);
    
    // Handle client disconnect
    req.on('close', () => {
        clients.delete(client);
        console.log(`Client disconnected. Total clients: ${clients.size}`);
    });
});

export type ConnectionId = string;
export interface MessageResponse {
    connId: ConnectionId;
    event: string;
    data: any;
}

app.post('/events', express.json(), (req: Request, res: Response) => {
    req.body
});

// Broadcast message to interested clients only
function broadcast(event: string, data: any) {
    const message = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
    
    clients.forEach((client) => {
        // Send to client if no filter is set OR if event is in their filter
        if (client.eventFilter.length === 0 || client.eventFilter.includes(event)) {
            client.response.write(message);
        }
    });
}

// Send periodic updates every 2 seconds
let counter = 0;
setInterval(() => {
    counter++;
    broadcast('message', {
        counter,
        timestamp: new Date().toISOString(),
        message: `Server update #${counter}`
    });
}, 5000);

// Send keepalive every 30 seconds to prevent connection timeout
setInterval(() => {
    broadcast('keepalive', {
        timestamp: new Date().toISOString()
    });
}, 10000);

// Health check endpoint
app.get('/health', (req: Request, res: Response) => {
    res.json({
        status: 'ok',
        clients: clients.size,
        uptime: process.uptime()
    });
});

// Start server
app.listen(PORT, HOSTNAME, () => {
    console.log(`SSE Server running on http://${HOSTNAME}:${PORT}`);
    console.log(`SSE endpoint: http://${HOSTNAME}:${PORT}/events`);
    console.log(`Health check: http://${HOSTNAME}:${PORT}/health`);
});

export { app, broadcast };
