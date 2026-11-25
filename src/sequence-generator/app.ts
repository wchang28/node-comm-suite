import express, { NextFunction, Request, Response } from 'express';
import path from 'node:path';
import morgan from 'morgan';
import helmet from 'helmet';
import { access, constants } from 'node:fs/promises';
import { FileBasedSequenceNumberGenerator } from '../file-based-sequence-number-generator.js';
// environment variables
// 1. PORT and LISTEN_HOST
// 2. SEQUENCE_JSON_FILE
// 3. AUTH_TOKEN
const PORT = Number(process.env.PORT ?? 8080);
const LISTEN_HOST = process.env.LISTEN_HOST ?? '0.0.0.0';
if (!Number.isInteger(PORT) || PORT <= 0 || PORT > 65535) {
    throw new Error(`Invalid PORT: ${process.env.PORT}`);
}

const sequenceFilePath = path.resolve(process.env.SEQUENCE_JSON_FILE ?? 'counter.json');
const sequenceGenerator = new FileBasedSequenceNumberGenerator(sequenceFilePath);
sequenceGenerator
.on("started", () => {
    console.log("Sequence number generator <<started>>");
}).on("stopped", () => {
    console.log("Sequence number generator <<stopped>>");
}).on("stopping", () => {
    console.log("Sequence number generator <<stopping>>");
})
.start();

const AUTH_TOKEN = process.env.AUTH_TOKEN ?? '';

if (!AUTH_TOKEN) {
    console.log('Warning: AUTH_TOKEN is not set. Anyone can access the API.');
}

const authenticateToken = (req: Request, res: Response, next: NextFunction) => {
    const token = req.query.token as string;
    if (AUTH_TOKEN && token !== AUTH_TOKEN) {
        res.status(401).json({ message: 'Unauthorized' });
        return;
    }
    next();
};

const app = express();
app.disable('x-powered-by');
app.set('trust proxy', true);
app.use(helmet());
app.use(morgan('combined'));

app.use((req: Request, res: Response, next) => {
    res.setHeader('Cache-Control', 'no-store');
    res.setHeader('Access-Control-Allow-Origin', '*');
    next();
});

// Health check endpoint
app.get('/health', async (req: Request, res: Response) => {
    try {
        await access(path.dirname(sequenceFilePath), constants.W_OK);
        res.json({ status: 'ok' });
    } catch {
        res.status(503).json({ status: 'unavailable' });
    }
});

app.use(authenticateToken);

app.get('/', async (req: Request, res: Response) => {
    const nextValue = await sequenceGenerator.next();
    res.json(nextValue);
});

app.get('/next', async (req: Request, res: Response) => {
    const nextValue = await sequenceGenerator.next();
    res.json(nextValue);
});

app.get('/reset', async (req: Request, res: Response) => {
    const nextValue = await sequenceGenerator.reset();
    res.json(nextValue);
});

app.get('/current', async (req: Request, res: Response) => {
    const currentValue = await sequenceGenerator.current();
    res.json(currentValue);
});

app.get('/set', async (req: Request, res: Response) => {
    const v = Number(req.query.value);
    if (!Number.isInteger(v)) {
        res.status(400).json({ error: 'Invalid value' });
        return;
    }
    const value = await sequenceGenerator.set(v);
    res.json(value);
});

app.get("/status", async (req: Request, res: Response) => {
    const current = await sequenceGenerator.current();
    const { state, processing, pending, jobs: jobList } = sequenceGenerator.toJSON();
    const jobs = jobList.map(({state, input}) => {
        return {
            state,
            operation: input.operation,
            value: input.value ? input.value : null
        }
    });
    res.json({
        current,
        state,
        processing,
        pending,
        jobs
    });
});

const server = app.listen(PORT, LISTEN_HOST, (err?: Error) => {
    if (err) {
        console.error('Failed to start server:', err);
        process.exit(1);
    }
    console.log(`Sequence number generator running on http://${LISTEN_HOST}:${PORT}/`);
    console.log(`Sequence file: ${sequenceFilePath}`);
});

for (const sig of ['SIGTERM', 'SIGINT'] as const) {
    process.once(sig, () => {
        console.log(`${sig} received, shutting down`);
        const serverClosedPromise = new Promise<void>((resolve) => {
            console.log(`app server <<closing>>`);
            server.closeAllConnections();
            server.close(() => {
                console.log(`app server <<closed>>`);
                resolve();
            });
        });
        const promises = Promise.all([
            serverClosedPromise,
            sequenceGenerator.stop()
        ]);
        promises.then(() => {
            console.log(`Shutdown complete.`);
            process.exit(0);
        })
        setTimeout(() => process.exit(1), 10_000).unref();
    });
}