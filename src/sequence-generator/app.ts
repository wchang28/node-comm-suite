import express, { Request, Response } from 'express';
import path from 'node:path';
import morgan from 'morgan';
import helmet from 'helmet';
import { access, constants } from 'node:fs/promises';
import { FileBasedSequenceNumberGenerator } from '../file-based-sequence-number-generator.js';

const sequenceFilePath = path.resolve(process.env.SEQUENCE_FILE ?? 'sequence.txt');
const PORT = Number(process.env.PORT ?? 8080);
const HOSTNAME = process.env.HOSTNAME ?? '0.0.0.0';
if (!Number.isInteger(PORT) || PORT <= 0 || PORT > 65535) {
    throw new Error(`Invalid PORT: ${process.env.PORT}`);
}

const sequenceGenerator = new FileBasedSequenceNumberGenerator(sequenceFilePath);

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

app.get('/current', async (req: Request, res: Response) => {
    const currentValue = await sequenceGenerator.current();
    res.json(currentValue);
});

app.get('/next', async (req: Request, res: Response) => {
    const nextValue = await sequenceGenerator.next();
    res.json(nextValue);
});

const server = app.listen(PORT, HOSTNAME, (err?: Error) => {
    if (err) {
        console.error('Failed to start server:', err);
        process.exit(1);
    }
    console.log(`Sequence number generator running on http://${HOSTNAME}:${PORT}/ (file: ${sequenceFilePath})`);
});

for (const sig of ['SIGTERM', 'SIGINT'] as const) {
    process.once(sig, () => {
        console.log(`${sig} received, shutting down`);
        server.close(() => process.exit(0));
        setTimeout(() => process.exit(1), 10_000).unref();
    });
}