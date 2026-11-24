import express from 'express';
import {RequestGetSet} from '../../request-get-set.js';
import { router as objRouter } from './obj-router.js';
import type {NodeId} from '../../../types.js';
import { dispatcherControlPermissionMiddleware } from '../permission.js';

const router = express.Router();
export { router };

const endware = async (req: express.Request, res: express.Response) => {
    const {dispatcher} = new RequestGetSet(req);
    res.json(await dispatcher.nodesTrackerJSON());
};

router.get("/", endware);

router.get("/enable-all", dispatcherControlPermissionMiddleware, async (req: express.Request, res: express.Response, next: express.NextFunction) => {
    const {dispatcher} = new RequestGetSet(req);
    await dispatcher.setNodesEnabled(null, true);
    next();
}, endware);

router.get("/disable-all", dispatcherControlPermissionMiddleware, async (req: express.Request, res: express.Response, next: express.NextFunction) => {
    const {dispatcher} = new RequestGetSet(req);
    await dispatcher.setNodesEnabled(null, false);
    next();
}, endware);

router.use('/:nodeId', async (req: express.Request, res: express.Response, next: express.NextFunction) => {
    const nodeId = req.params.nodeId as NodeId;
    const req_gs = new RequestGetSet(req);
    const {dispatcher} = req_gs;
    const node = await dispatcher.nodeJSON(nodeId);
    if (!node) {
        res.status(404).json({message: `Node (${nodeId}) not found`});
    } else {
        req_gs.node = node;
        next();
    }
}, objRouter);