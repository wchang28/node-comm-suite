import express from 'express';
import {RequestGetSet} from '../../request-get-set.js';
import { router as objRouter } from './obj-router.js';
import type {NodeId, NodeInfoRow} from '../../../types.js';

const router = express.Router();
export { router };

router.get("/", async (req: express.Request, res: express.Response) => {
    const {dispatcher} = new RequestGetSet(req);
    const dispatcherJSON = await dispatcher.toJSON();
    res.json(dispatcherJSON.nodes);
});



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