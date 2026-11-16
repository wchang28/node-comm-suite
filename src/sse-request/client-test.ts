import {Client} from "./client.js";
import {RequestContent, RequestResponse} from "./types.js";
import os from 'os';
import fs from 'fs';
import { generateGuid, sleep } from "../utils.js";
import { OSUtils } from "node-os-utils";
import { SystemLoadStatus, SystemLoadingInfo } from "../type.js";

console.log(`hostname=${os.hostname()}`);
console.log(`availableCPUs=${os.cpus().length}`);
const totalMem = os.totalmem();
const freeMem = os.freemem();
const usedMem = totalMem - freeMem;
const {rss, heapTotal, heapUsed} = process.memoryUsage();

// Path can be any directory or mount point (e.g., '/mnt/c' or 'C:/')
const stats = fs.statfsSync('/mnt/c');

const blockSize = stats.bsize;
const totalSpace = stats.blocks * blockSize;    // Total size of the filesystem
const freeSpace = stats.bfree * blockSize; // Physical free
const availableSpace = stats.bavail * blockSize; // Available to current user

console.log(`CPUs: ${os.cpus().length}`);
console.log(`Total: ${(totalMem / 1024 / 1024 / 1024).toFixed(2)} GB`);
console.log(`Free: ${(freeMem / 1024 / 1024 / 1024).toFixed(2)} GB`);
console.log(`Used: ${(usedMem / 1024 / 1024 / 1024).toFixed(2)} GB`);
console.log(`Process RSS: ${(rss / 1024 / 1024).toFixed(2)} MB`);
console.log(`Heap Total: ${(heapTotal / 1024 / 1024).toFixed(2)} MB`);
console.log(`Heap Used: ${(heapUsed / 1024 / 1024).toFixed(2)} MB`);

async function getSystemLoading(cpuWeight: number = 0.5, memWeight: number = 0.5) {
    let cpuUsagePercent = 0.0;
    const osutils = new OSUtils();
    const cpuUsage = await osutils.cpu.usage();
    if (cpuUsage.success) {
        cpuUsagePercent = cpuUsage.data;
    }
    const totalMem = os.totalmem();
    const freeMem = os.freemem();
    const usedMem = totalMem - freeMem;
    const memUsagePercent = (usedMem / totalMem) * 100;
    let loadingScore = (cpuUsagePercent * cpuWeight) + (memUsagePercent * memWeight);
    if (loadingScore < 1) loadingScore = 1;
    if (loadingScore > 100) loadingScore = 100;
    const loadStatus: SystemLoadStatus = (loadingScore > 90 ? 'critical' : loadingScore > 75 ? 'high' : 'normal');
    const ret: SystemLoadingInfo = {
        cpuUsagePercent,
        memUsagePercent,
        loadingScore,
        loadStatus
    };
    return ret;
}

async function testRequestProcessor(request: RequestContent) {
    const {path, method} = request;
    if (path === "/identify" && method === "GET") {
        const response: RequestResponse = {
            status: 200,
            headers: {"content-type": "application/json"},
            body: {
                sessionId: generateGuid(),
                hostname: os.hostname(),
                availableCPUs: os.cpus().length - 2,
                currentLoading: await getSystemLoading()
            }
        };
        return response;
    } else {
        const response: RequestResponse = {
            status: 404,
            headers: {"content-type": "application/json"},
            body: {
                message: `Unsupported request. Path: ${path}, Method: ${method}`
            }
        };
        return response;
    }
}

const sseClient = new Client("http://127.0.0.1:80/task-launcher/events", testRequestProcessor);
sseClient.on("state-change", (newState) => {
    console.log(`Client state changed: ${newState}`);
    if (newState === "ready") {
        console.log(`Client is ready. Connection ID: ${sseClient.connId}`);
    }
}).on("connection-id-change", (oldConnectId, newConnectId) => {
    console.log(`<<connection-id-change>>. ${oldConnectId} ==> ${newConnectId}`);
}).on("message", (data, event, id) => {
    console.log(`Received <<message>>. Event: "${event}", ID: ${id}, Data:`, JSON.stringify(data));
}).on("connection-id", (connId) => {
    console.log(`Received <<connection-id>>. ConnId: ${connId}`);
}).on("ping", (timestamp) => {
    console.log(`Received <<ping>> at timestamp: ${timestamp}`);
}).on("request", (connId, reqId, content) => {
    console.log(`Received <<request>>. ConnId: ${connId}, ReqId: ${reqId}, Content:`, JSON.stringify(content));
}).on("request-acked", (connId, reqId, response, reply) => {
    console.log(`<<request-acked>>. ConnId: ${connId}, ReqId: ${reqId}, Response:`, JSON.stringify(response), "Reply:", JSON.stringify(reply));
}).on("request-ack-error", (connId, reqId, error) => {
    console.error(`<<request-ack-error>>. ConnId: ${connId}, ReqId: ${reqId}, Error:`, error);
});

sseClient.connect();
/*
async function test() {
    for (let i = 0; i < 100; i++) {
        console.log(`Iteration ${i + 1}`);
        await sseClient.connect();
        await sleep(3000);
        sseClient.close();
    }
}

test().then(() => {
    console.log("Test completed successfully.");
}).catch((err) => {
    console.error("Test failed with error:", err);
});
*/