import express from 'express';
import {RequestGetSet} from '../../request-get-set.js';
import { dispatcherControlPermissionMiddleware } from '../permission.js';

const router = express.Router();
export { router };

router.get("/", async (req: express.Request, res: express.Response) => {
    const {dispatcher} = new RequestGetSet(req);
    const dispatcherJSON = await dispatcher.toJSON();
    res.json(dispatcherJSON.nodes);
});