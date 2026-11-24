import express from 'express';
import {RequestGetSet} from '../../request-get-set.js';
import { dispatcherControlPermissionMiddleware } from '../permission.js';

const router = express.Router();
export { router };

router.get("/",  (req: express.Request, res: express.Response) => {
    const {node} = new RequestGetSet(req);
    res.json(node);
});