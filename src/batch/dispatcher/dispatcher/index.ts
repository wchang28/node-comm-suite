import express from 'express';
import {RequestGetSet} from '../request-get-set.js';
import { dispatcherControlPermissionMiddleware } from './permission.js';
import { router as queueRouter } from './queue/index.js';
import { router as nodeRouter } from './node/index.js';

const router = express.Router();
export { router };

router.get("/", async (req: express.Request, res: express.Response) => {
    const {dispatcher} = new RequestGetSet(req);
    res.json(await dispatcher.toJSON());
});

router.get("/enable", dispatcherControlPermissionMiddleware, async (req: express.Request, res: express.Response) => {
    const {dispatcher} = new RequestGetSet(req);
    await dispatcher.enable();
    res.json(await dispatcher.toJSON());
});

router.get("/disable", dispatcherControlPermissionMiddleware, async (req: express.Request, res: express.Response) => {
    const {dispatcher} = new RequestGetSet(req);
    await dispatcher.disable();
    res.json(await dispatcher.toJSON());
});

router.use('/queue', queueRouter);  
router.use('/node', nodeRouter);  