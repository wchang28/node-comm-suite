import { TaskLauncherApp } from "./task-launcher-app.js";
import { createDBPool } from "./db-pools.js";
import { DBConfigsFile, toDBConfig } from "../../db-types.js";
import { RequestBasedSystemLoadingLogger } from "../request-based-system-loading-logger.js";
import fs from 'node:fs';

// DB settings come from a JSON file (default ./db-configs.json, override with DB_CONFIGS_PATH)
const dbConfigsPath = process.env.DB_CONFIGS_PATH ?? "db-configs.json";
const dbConfig = toDBConfig(JSON.parse(fs.readFileSync(dbConfigsPath, "utf8")) as DBConfigsFile);
console.log(`Using DB type: ${dbConfig.type}, schema: ${dbConfig.schema} (from ${dbConfigsPath})`);
const dbPool = createDBPool(dbConfig.type, dbConfig.connectConfig, dbConfig.schema);

const systemLoadPoolingIntervalMS = 1000; // TODO: read from env vars or config file
const systemLoadingLogger = new RequestBasedSystemLoadingLogger("http://127.0.0.1:80/system-loading-logger");

const app = new TaskLauncherApp("http://127.0.0.1:8080/task-launcher/events", dbPool, systemLoadingLogger, "0.0.0.0", 3000, systemLoadPoolingIntervalMS, 2);
app.on("listening", (listenAddr) => {
    console.log(`app is listening on ${listenAddr}`);
    console.log(`app health url: ${listenAddr}/health`);
}).on("client-state-change", (newState) => {
    console.log(`Client state changed: ${newState}`);
}).on("client-ready", () => {
    console.log(`Client is <<ready>>. Connection ID: ${app.connId}`);
}).on("connection-id-change", (oldConnId, newConnId) => {
    console.log(`<<connection-id-change>>. ${oldConnId} ==> ${newConnId}`);
}).on("connection-id", (connId) => {
    console.log(`<<connection-id>>. ConnId: ${connId}`);
}).on("ping", (timestamp) => {
    console.log(`<<ping>> received from client at ${timestamp}`);
}).on("task-dispatch-received", (j, t) => {
    console.log(`<<task-dispatch-received>> Task (${j}, ${t}) dispatch received.`);
}).on("task-launched", (j, t, pid) => {
    console.log(`<<task-launched>> Task (${j}, ${t}) launched. pid=${pid}`);
}).on("task-finished", (j, t, pid, durationMS, code) => {
    console.log(`<<task-finished>> Task (${j}, ${t}) finished. pid=${pid}, durationMS=${durationMS}, code=${code}`);
}).on("task-launch-error", (j, t, errorType, cause) => {
    console.log(`<<task-launch-error>> Failed to launch task (${j}, ${t}). Error: ${errorType}, Cause: ${cause}`);
}).on("job-processes-kill-received", (jobId) => {
    console.log(`<<job-processes-kill-received>> Received request to kill processes for job ${jobId}`);
}).on("job-processes-killed", (jobId, numProcesses) => {
    if (numProcesses > 0) {
        console.log(`<<job-processes-killed>> for job ${jobId}. Number of processes killed: ${numProcesses}`);
    } else {
        console.log(`<<job-processes-killed>> for job ${jobId}. No processes were killed.`);
    }
}).on("system-load-pooled", (info) => {
    //console.log(`<<system-load-pooled>> current loading: ${JSON.stringify(info)}`);
}).on("system-load-monitor-error", (error) => {
    console.error(`<<system-load-monitor-error>> System load monitor error: ${error.message}`);
});
app.start();    // Start the app (and the underlying HTTP server)