import express from 'express';
import {RequestGetSet} from '../../request-get-set.js';
import { dispatcherControlPermissionMiddleware } from '../permission.js';

const router = express.Router();
export { router };

router.get("/", async (req: express.Request, res: express.Response) => {
    const {dispatcher} = new RequestGetSet(req);
    const dispatcherJSON = await dispatcher.toJSON();
    res.json(dispatcherJSON.queue);
});

router.get("/close", dispatcherControlPermissionMiddleware, async (req: express.Request, res: express.Response) => {
    const {dispatcher} = new RequestGetSet(req);
    await dispatcher.closeQueue();
    const dispatcherJSON = await dispatcher.toJSON();
    res.json(dispatcherJSON.queue);
});

router.get("/open", dispatcherControlPermissionMiddleware, async (req: express.Request, res: express.Response) => {
    const {dispatcher} = new RequestGetSet(req);
    await dispatcher.openQueue();
    const dispatcherJSON = await dispatcher.toJSON();
    res.json(dispatcherJSON.queue);
});
