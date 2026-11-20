import express from 'express';
import {RequestGetSet} from '../request-get-set.js';
import { PoolPermission } from '../pool-permission.js';

const router = express.Router();
export { router };

router.get("/", async (req: express.Request, res: express.Response) => {
    const req_gs = new RequestGetSet(req);
    const {dispatcher} = req_gs;
    res.json(await dispatcher.toJSON());
});

const dispatcherControlPermissionMiddleware = (req: express.Request, res: express.Response, next: express.NextFunction) => {
    const req_gs = new RequestGetSet(req);
    const {user} = req_gs;
    if (!user || !(new PoolPermission(user).canControlDispatcher)) {
        return res.status(403).json({message: 'Permission denied'});
    }
    next();
};

router.get("/close-queue", dispatcherControlPermissionMiddleware, async (req: express.Request, res: express.Response) => {
    const req_gs = new RequestGetSet(req);
    const {dispatcher} = req_gs;
    res.json({closed: await dispatcher.closeQueue()});
});
``
router.get("/open-queue", dispatcherControlPermissionMiddleware, async (req: express.Request, res: express.Response) => {
    const req_gs = new RequestGetSet(req);
    const {dispatcher} = req_gs;
    res.json({closed: await dispatcher.openQueue()});
});

router.get("/enable", dispatcherControlPermissionMiddleware, async (req: express.Request, res: express.Response) => {
    const req_gs = new RequestGetSet(req);
    const {dispatcher} = req_gs;
    res.json({ enabled: await dispatcher.enable() });
});

router.get("/disable", dispatcherControlPermissionMiddleware, async (req: express.Request, res: express.Response) => {
    const req_gs = new RequestGetSet(req);
    const {dispatcher} = req_gs;
    res.json({ enabled: await dispatcher.disable() });
});