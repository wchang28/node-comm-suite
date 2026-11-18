import {parser} from 'stream-json';
import {streamArray} from 'stream-json/streamers/stream-array.js';
import chain from 'stream-chain';
import fs from 'node:fs';
import {ObjectTransformStream} from "./streams/object-transform-stream.js";
import {PagifyStream} from "./streams/pagification.js";
import {JobId, Task, TaskIndex} from "./batch/types.js";
import {JsonStringifyStream} from "./streams/json-stringify-stream.js";
import {StringWritable}  from "./streams/string-stream.js";

 interface TaskRow {
    j: JobId;
    t: TaskIndex;
    cmd: string;
    tag?: string | null;
    envJSON?: string | null;
    stdin?: string | null;
}

function test() {
    let jobId: JobId;
    let first:boolean = true;
    let numTasks: number = 0;
    //const res = fs.createWriteStream('/mnt/q/xfer/4wen/grid-tasks-out.json');
    const res = new StringWritable();
    const pipeline = chain([
        fs.createReadStream('/mnt/q/xfer/4wen/grid-tasks.json'), // a file far bigger than RAM is fine
        parser(),
        streamArray(), // emit one array element at a time
        new ObjectTransformStream<Task, Task>(async (task) => {
            if (first) {
                console.log('creating a new job');
                jobId = "31";
                first = false;
            }
            return task;
        }),
        new PagifyStream<Task>(80),
        new ObjectTransformStream<Task[], TaskRow[]>(async (tasks) => {
            const taskRows: TaskRow[] = [];
            for (let i = 0; i < tasks.length; i++) {
                const {cmd, tag, envJSON, stdin} = tasks[i];
                const taskRow: TaskRow = {
                    j: jobId,
                    t: numTasks + i,
                    cmd: cmd,
                    tag: tag ?? null,
                    envJSON: envJSON ?? null,
                    stdin: stdin ?? null
                };
                taskRows.push(taskRow);
            }
            numTasks += tasks.length;
            return taskRows;
        }),
        new ObjectTransformStream<TaskRow[], string>(async (taskRows) => `[${taskRows[0].t},${taskRows[taskRows.length - 1].t+1})`),
        new JsonStringifyStream(),
        res
    ]);
    pipeline.on('data', (value) => {
        //count++;
        //totalTasks += value;
        console.log(value);
    });
    pipeline.on('end', () => console.log(`Done, numTasks: ${numTasks}`));
    res.on('finish', () => {
        console.log('Write stream finished');
        console.log(res.text);
    });
}

test();