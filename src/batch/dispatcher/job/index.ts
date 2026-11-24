import express from 'express';
import {RequestGetSet} from '../request-get-set.js';
import {error_message} from '../../../utils.js';
import { router as objRouter, job_appendTasksPage, job_submit } from './obj-router.js';
import {ObjectTransformStream} from "../../../streams/object-transform-stream.js";
import {PagifyStream} from "../../../streams/pagification.js";
import {parser} from 'stream-json';
import {streamArray} from 'stream-json/streamers/stream-array.js';
import chain from 'stream-chain';
import { JobId, Task } from '../../types.js';
import { JsonStringifyStream } from '../../../streams/json-stringify-stream.js';
import {StringWritable} from '../../../streams/string-stream.js';

const router = express.Router();
export { router };

router.get("/most-recent", async (req, res) => {
    const {poolDataPlaneDBPool, defaultMostRecentJobsLimit} = new RequestGetSet(req);
    const limit = parseInt(req.query.limit as string) || defaultMostRecentJobsLimit;
    const jobs = await poolDataPlaneDBPool.getMostRecentJobs(limit);
    res.json(jobs);
});

interface CreateNewJobParams {
    description?: string | null;
    tag?: string | null;
}

async function createNewJob(req: express.Request, creatParams: CreateNewJobParams) {
    const {description, tag} = creatParams;
    const req_gs = new RequestGetSet(req);
    const {user, poolDataPlaneDBPool} = req_gs;
    return await poolDataPlaneDBPool.createNewJob(user.id, user.name, description ?? null, tag ?? null);
}

router.put('/', express.json(), async (req, res) => {
    try {
        const job = await createNewJob(req, req.body as CreateNewJobParams);
        res.status(200).json(job);
    } catch (error) {
        res.status(500).json({message: error_message(error)});
    }
});

router.post("/submit", async (req, res) => {
    const description = req.query.description as string | undefined;
    const tag = req.query.tag as string | undefined;
    const createParams: CreateNewJobParams = {description, tag};
    const req_gs = new RequestGetSet(req);
    const {poolDataPlaneDBPool} = req_gs;
    let jobIdCreated: JobId | null = null;
    try {
        const {jobId} = await createNewJob(req, createParams);
        jobIdCreated = jobId;
        const stringWritable = new StringWritable();
        const pipeline_finished = new Promise<number>((resolve, reject) => {
            const pipeline = chain([
                req,
                parser(),
                streamArray(),
                new ObjectTransformStream<{key: number, value: Task}, Task | null>(async (input) => {
                    return input.value ? input.value : null;
                }),
                new PagifyStream<Task|null>(100),
                new ObjectTransformStream<(Task| null)[], number|null>(async (tsks) => {
                    try {
                        const tasks: Task[] = [];
                        for (const task of tsks) {
                            if (!task) {
                                throw new Error('Task is null or undefined');
                            } else {
                                tasks.push(task);
                            }
                        }
                        const updatedJob = await job_appendTasksPage(req, jobId, tasks);
                        return updatedJob.numTasks;
                    } catch (error) {
                        return null;
                    }
                }),
                new JsonStringifyStream(),
                stringWritable
            ]);
            pipeline.on('end', () =>  {
                const pagenationResult: (number | null)[] = JSON.parse(stringWritable.text);
                let badResult = false;
                for (const result of pagenationResult) {
                    if (result === null) {
                        badResult = true;
                        break;
                    }
                }
                const numTasksAppended = pagenationResult[pagenationResult.length - 1] as number;
                if (badResult) {
                    reject(new Error(`Failed to append some tasks to job ${jobId}, pagenationResult=${pagenationResult}`));
                } else {
                    resolve(numTasksAppended);
                }
            }).on('error', (error) => {
                reject(error);
            });
        });
        const numTasksAppended = await pipeline_finished;
        const job = await poolDataPlaneDBPool.getJob(jobId);
        if (numTasksAppended !== job.numTasks) {
            throw new Error(`Mismatch in number of tasks appended for job ${jobId}: expected ${numTasksAppended}, got ${job.numTasks}`);
        }
        await job_submit(req, job); // submit the job
        res.status(200).json(job);
    } catch (error) {
        if (jobIdCreated !== null) {
            try {await poolDataPlaneDBPool.deleteJob(jobIdCreated);} catch (deleteError) {console.error(error_message(deleteError));}
            res.status(500).json({message: error_message(error)});
        } else {
            res.status(500).json({message: `Unable to create job: ${error_message(error)}`});
        }
    }
});

router.use('/:jobId', async (req, res, next) => {
    const req_gs = new RequestGetSet(req);
    const {poolDataPlaneDBPool} = req_gs;
    try {
        const jobId = req.params.jobId as JobId;
        const job = await poolDataPlaneDBPool.getJob(jobId);
        req_gs.job = job;
        next();
    } catch (error) {
        res.status(404).json({message: error_message(error)});
    }
}, objRouter);