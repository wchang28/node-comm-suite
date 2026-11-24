import express from 'express';
import {RequestGetSet} from '../../request-get-set.js';
import { dispatcherControlPermissionMiddleware } from '../permission.js';

const router = express.Router();
export { router };

const endware = async (req: express.Request, res: express.Response) => {
    const {dispatcher} = new RequestGetSet(req);
    res.json(await dispatcher.queueJSON());
};

router.get("/", endware);

router.get("/close", dispatcherControlPermissionMiddleware, async (req: express.Request, res: express.Response, next: express.NextFunction) => {
    const {dispatcher} = new RequestGetSet(req);
    await dispatcher.closeQueue();
    next();
}, endware);

router.get("/open", dispatcherControlPermissionMiddleware, async (req: express.Request, res: express.Response, next: express.NextFunction) => {
    const {dispatcher} = new RequestGetSet(req);
    await dispatcher.openQueue();
    next();
}, endware);
