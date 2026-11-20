import {Client} from "../../sse-request/client.js";

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
    console.table(data);
}).on("ping", (timestamp) => {
    console.log("Ping received at:", timestamp);
}).connect();
