import express from 'express';
import {RequestGetSet} from '../request-get-set.js';
import { JobId, Job, Task} from '../../types.js';
import { error_message } from '../../../utils.js';
import { PoolPermission } from '../pool-permission.js';

const router = express.Router();
export { router };

router.get('/', (req, res) => {
    const req_gs = new RequestGetSet(req);
    res.status(200).json(req_gs.job);
});

export async function job_appendTasksPage(req: express.Request, jobId: JobId, tasks: Task[]) {
    if (!tasks || tasks.length === 0) {
        throw new Error('No tasks provided');
    }
    const req_gs = new RequestGetSet(req);
    const {poolDataPlaneDBPool} = req_gs;
    return await poolDataPlaneDBPool.appendTasksPageToJob(jobId, tasks);
}

export async function job_submit(req: express.Request, job: Job) {
    const req_gs = new RequestGetSet(req);
    const {dispatcher} = req_gs;
    const jobId = job.jobId;
    const numTasks = job.numTasks;
    await dispatcher.queueJob(jobId, numTasks); // Queue the job for processing
}

router.post("/append-tasks-page", express.json(), async (req, res) => {
    try {
        const tasks: Task[] = req.body;
        const req_gs = new RequestGetSet(req);
        const {job} = req_gs;
        const updatedJob = await job_appendTasksPage(req, job.jobId, tasks);
        res.status(200).json(updatedJob);
    } catch (error) {
        res.status(500).json({message: error_message(error)});
    }
});

router.get("/submit", async (req, res) => {
    try {
        const req_gs = new RequestGetSet(req);
        const {job} = req_gs;
        await job_submit(req, job);
        res.status(200).json(job);
    } catch (error) {
        res.status(500).json({message: error_message(error)});
    }
});

const killJobPermissionMiddleware = (req: express.Request, res: express.Response, next: express.NextFunction) => {
    const req_gs = new RequestGetSet(req);
    const {user, job} = req_gs;
    if (!user || !(new PoolPermission(user).canKillJob(job))) {
        return res.status(403).json({message: 'Permission denied'});
    }
    if (job.status == "ABORTED" || job.status == "FINISHED") {
        return res.status(400).json({message: 'Cannot kill a job that is already aborted or finished'});
    }
    next();
};

router.get("/kill", killJobPermissionMiddleware, async (req, res) => {
    try {
        const req_gs = new RequestGetSet(req);
        const {job, user, poolDataPlaneDBPool, dispatcher} = req_gs;
        await poolDataPlaneDBPool.abortJob(user.id, user.name, job.jobId);
        await dispatcher.killJob(job.jobId);
        const killedJob = await poolDataPlaneDBPool.getJob(job.jobId);
        res.status(200).json(killedJob);
    } catch (error) {
        res.status(500).json({message: error_message(error)});
    }
});

router.get("/trigger-kill", killJobPermissionMiddleware, async (req, res) => {
    try {
        const req_gs = new RequestGetSet(req);
        const {job, user, poolDataPlaneDBPool, dispatcher} = req_gs;
        const abortedJob = await poolDataPlaneDBPool.abortJob(user.id, user.name, job.jobId);
        dispatcher.killJob(job.jobId);
        res.status(200).json(abortedJob);
    } catch (error) {
        res.status(500).json({message: error_message(error)});
    }
});

router.get("/resubmit", async (req, res) => {
    try {
        const req_gs = new RequestGetSet(req);
        const {job, user, poolDataPlaneDBPool} = req_gs;
        const failedTasksOnly = (req.query.failedTasksOnly === 'true');
        const newJob = await poolDataPlaneDBPool.replicateJob(user.id, user.name, job.jobId, failedTasksOnly);
        await job_submit(req, newJob);
        res.status(200).json(newJob);
    } catch (error) {
        res.status(500).json({message: error_message(error)});
    }
});