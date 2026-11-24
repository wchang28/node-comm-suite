import express from 'express';
import {RequestGetSet} from '../../request-get-set.js';
import { dispatcherControlPermissionMiddleware } from '../permission.js';
import { NodeInfoRow } from '../../../types.js';

const router = express.Router();
export { router };

router.get("/",  (req: express.Request, res: express.Response) => {
    const {node} = new RequestGetSet(req);
    res.json(node);
});

const finalware = async (req: express.Request, res: express.Response) => {
    const {node, dispatcher} = new RequestGetSet(req);
    const nodeUpdated = await dispatcher.nodeJSON(node.id);
    if (!nodeUpdated) {
        res.status(404).json({message: `Node (${node.id}) not found`});
    } else {
        res.json(nodeUpdated);
    }
};

router.get("/enable", dispatcherControlPermissionMiddleware, async (req: express.Request, res: express.Response, next: express.NextFunction) => {
    const {node, dispatcher} = new RequestGetSet(req);
    await dispatcher.enableNode(node.id);
    next();
}, finalware);

router.get("/disable", dispatcherControlPermissionMiddleware, async (req: express.Request, res: express.Response, next: express.NextFunction) => {
    const {node, dispatcher} = new RequestGetSet(req);
    await dispatcher.disableNode(node.id);
    next();
}, finalware);
