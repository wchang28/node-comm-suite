import express from 'express';
import { RequestGetSet } from '../request-get-set.js';
import { PoolPermission } from '../pool-permission.js';

export const dispatcherControlPermissionMiddleware = (req: express.Request, res: express.Response, next: express.NextFunction) => {
    const {user} = new RequestGetSet(req);
    if (!user || !(new PoolPermission(user).canControlDispatcher)) {
        return res.status(403).json({message: 'Permission denied'});
    }
    next();
};