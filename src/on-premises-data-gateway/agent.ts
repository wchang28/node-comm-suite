import * as fs from "fs";
import {Client} from "../sse-request/client.js";
import {RequestContent, RequestResponse} from "../sse-request/types.js";
import {error_message, is_json_content} from "../utils.js";

// Environment Configuration
// PROXY_SERVER_BASE_URL
// RMBS_API_BASE_URL
const PROXY_SERVER_BASE_URL: string = (process.env.PROXY_SERVER_BASE_URL ? process.env.PROXY_SERVER_BASE_URL : `http://localhost:8080`);
console.log(`PROXY_SERVER_BASE_URL=${PROXY_SERVER_BASE_URL}`);
const ROUTE_MAP_DEF_JSON: string = (process.env.ROUTE_MAP_DEF_JSON ? process.env.ROUTE_MAP_DEF_JSON : `route-map.json`);
console.log(`ROUTE_MAP_DEF_JSON=${ROUTE_MAP_DEF_JSON}`);

interface RouteMapItem {
    routePath: string;
    baseUrl: string;
}
type RouteMapDefs = RouteMapItem[];

// load and validate a RouteMapDefs JSON file, e.g.
// [{"routePath": "/some-route-path", "baseUrl": "http://localhost:8080"}]
function loadRouteMapDefs(filePath: string): RouteMapDefs {
    const content = fs.readFileSync(filePath, "utf8");
    let parsed: unknown;
    try {
        parsed = JSON.parse(content);
    } catch (error) {
        throw new Error(`Invalid JSON in route map file ${filePath}: ${error_message(error as any)}`);
    }
    if (!Array.isArray(parsed)) {
        throw new Error(`Route map file ${filePath} must contain a JSON array`);
    }
    return parsed.map((item, i) => {
        if (!item || typeof item !== "object" || typeof item.routePath !== "string" || typeof item.baseUrl !== "string") {
            throw new Error(`Route map file ${filePath}: item ${i} must have string "routePath" and "baseUrl"`);
        }
        const {routePath, baseUrl} = item as RouteMapItem;
        return {routePath, baseUrl};
    });
}

function loadRouteMapping(filePath: string) : {[key: string]: string} {
    const routeDefs = loadRouteMapDefs(filePath);
    const routeMap: {[key: string]: string} = {};
    for (const {routePath, baseUrl} of routeDefs) {
        routeMap[routePath] = baseUrl;
    }
    return routeMap;
}

let routeMapping: {[key: string]: string} = loadRouteMapping(ROUTE_MAP_DEF_JSON);
console.log(`routeMapping=\n${JSON.stringify(routeMapping, null, 2)}`);

async function handleApiRoutes(request: RequestContent) {
    const {path, method, body: reqBody, headers: hdrs} = request;
    for (const routePath in routeMapping) { // for each routePath in routeMap
        if (path.startsWith(routePath)) {   // if the request path starts with the routePath
            try {
                const api_path = path.substring(routePath.length);
                const baseUrl = routeMapping[routePath];
                const fetchUrl = `${baseUrl}${api_path}`;
                //console.log(`fetchUrl=${fetchUrl}`);
                const headers = hdrs || {};
                const reqInit: RequestInit = {
                    method,
                    headers,
                    body: (reqBody == undefined ? undefined : (typeof reqBody === "string" ? reqBody : JSON.stringify(reqBody)))
                }
                //console.log(`reqInit=${JSON.stringify(reqInit)}`);
                const response = await fetch(fetchUrl, reqInit);
                //console.log(`response.status=${response.status}`);
                const responseHeaders: {[key: string]: string} = {};
                response.headers.forEach((value, key) => {
                    responseHeaders[key] = value;
                });
                //console.log(`responseHeaders=${JSON.stringify(responseHeaders)}`);
                let resBody: any = null;
                const hasContent = ("content-type" in responseHeaders && responseHeaders["content-type"].length > 0);
                if (hasContent) {
                    if (is_json_content(responseHeaders["content-type"])) {
                        resBody = await response.json();
                    } else {
                        resBody = await response.text();
                    }
                }
                const res: RequestResponse = {
                    status: response.status,
                    headers: responseHeaders,
                    body: resBody
                };
                return res;
            } catch(error) {
                const res: RequestResponse = {
                    status: 500,
                    headers: {"content-type": "application/json"},
                    body: {
                        message: error_message(error as any)
                    }
                };
                return res;
            }
        }
    }
    const response: RequestResponse = {
        status: 404,
        headers: {"content-type": "application/json"},
        body: {
            message: `Unsupported request. Path: ${path}, Method: ${method}`
        }
    };
    return response;
}

async function requestProcessor(request: RequestContent) {
    const {path, method, body: reqBody, headers: hdrs} = request;
    // reqBody = string | object/null | undefined
    //console.log(`typeof reqBody=${typeof reqBody}, body=${reqBody}`);
    if (path === "/health" && method === "GET") {
        const response: RequestResponse = {
            status: 200,
            headers: {"content-type": "application/json"},
            body: {
                status: "ok"
            }
        };
        return response;
    }
    else if (path === "/route-mapping" && method === "GET") {
        const response: RequestResponse = {
            status: 200,
            headers: {"content-type": "application/json"},
            body: routeMapping
        };
        return response;
    }
    else if (path === "/route-mapping/reload" && method === "GET") {
        routeMapping = loadRouteMapping(ROUTE_MAP_DEF_JSON);
        const response: RequestResponse = {
            status: 200,
            headers: {"content-type": "application/json"},
            body: routeMapping
        };
        return response;
    } 
    else {
        return await handleApiRoutes(request);
    }
}

const sseClient = new Client(`${PROXY_SERVER_BASE_URL}/agent/events`, requestProcessor);

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