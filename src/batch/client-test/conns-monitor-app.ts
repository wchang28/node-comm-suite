import {Client} from "../../sse-request/client.js";
import {ConnectionId} from "../../sse-server.js";
import {ClientConnectionItem} from "../types.js";

const EVENT_URL = process.env.EVENT_URL || "http://localhost:8080/client-connections/events";

let myConnectionId: ConnectionId | null = null;

const client = new Client(EVENT_URL);
client.on("ready", () => {
    console.log("Client is ready");
}).on("connection-id", (connId) => {
    console.log("Connection ID received:", connId);
    myConnectionId = connId;
}).on("connection-id-change", (oldConnId, newConnId) => {
    console.log("Connection ID changed from", oldConnId, "to", newConnId);
}).on("event-notify", (event, data) => {
    console.log(`Event notify received: <<${event}>>`);
    const d = data as ClientConnectionItem[];
    const rows = d.map(item => ({ ...item, isMe: (myConnectionId != null && myConnectionId === item.connId) }));
    console.table(rows);
}).on("ping", (timestamp) => {
    console.log("Ping received at:", timestamp);
}).connect();
