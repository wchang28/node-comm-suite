import express from 'express';
import {RequestGetSet} from '../request-get-set.js';
import { dispatcherControlPermissionMiddleware } from './permission.js';
import { router as queueRouter } from './queue/index.js';
import { router as nodeRouter } from './node/index.js';

const router = express.Router();
export { router };

const endware = async (req: express.Request, res: express.Response) => {
    const {dispatcher} = new RequestGetSet(req);
    res.json(await dispatcher.toJSON());
};

router.get("/", endware);

router.get("/enable", dispatcherControlPermissionMiddleware, async (req: express.Request, res: express.Response, next: express.NextFunction) => {
    const {dispatcher} = new RequestGetSet(req);
    await dispatcher.enable();
    next();
}, endware);

router.get("/disable", dispatcherControlPermissionMiddleware, async (req: express.Request, res: express.Response, next: express.NextFunction) => {
    const {dispatcher} = new RequestGetSet(req);
    await dispatcher.disable();
    next();
}, endware);

router.use('/queue', queueRouter);  
router.use('/node', nodeRouter);  