import {Client} from "../../sse-request/client.js";
import { QueueJSON, NodesTrackerJSON, DispatcherStates, JobKillStatRow } from "../types.js";

const EVENT_URL = process.env.EVENT_URL || "http://localhost:8080/dispatcher/events";

const client = new Client(EVENT_URL);
client.on("ready", () => {
    console.log("Client is ready");
}).on("connection-id", (connId) => {
    console.log("Connection ID received:", connId);
}).on("connection-id-change", (oldConnId, newConnId) => {
    console.log("Connection ID changed from", oldConnId, "to", newConnId);
}).on("event-notify", (event, data) => {
    console.log(`Event notify received: <<${event}>>`);
    if (event === "queue-change") {
        const d = data as QueueJSON;
        console.log(`closed=${d.closed}`);
        console.log(`numJobs=${d.numJobs}`);
        console.log(`numTasks=${d.numTasks}`);
        console.table(d.jobs);
    } else if (event === "nodes-change") {
        const d = data as NodesTrackerJSON;
        console.log(`numNodes=${d.numNodes}`);
        console.log(`numAvailableTaskSlots=${d.numAvailableTaskSlots}`);
        console.log(`numUsedTaskSlots=${d.numUsedTaskSlots}`);
        console.log(`totalTaskSlots=${d.totalTaskSlots}`);
        console.log(`numUsableTaskSlots=${d.numUsableTaskSlots}`);
        console.log(`utilization=${d.utilization}`);
        console.table(d.nodes);
    } else if (event === "dispatcher-change") {
        const d = data as DispatcherStates;
        console.log(`enabled=${d.enabled}`);
        console.log(`dispatching=${d.dispatching}`);
    } else if (event === "job-killer-change") {
        const d = data as JobKillStatRow[];
        console.table(d);
    }
}).on("ping", (timestamp) => {
    console.log("Ping received at:", timestamp);
}).connect();
