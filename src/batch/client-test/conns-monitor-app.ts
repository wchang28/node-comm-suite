import {Client} from "../../sse-request/client.js";

const client = new Client("http://0.0.0.0:8080/connections-monitor/events");

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
});

client.connect();