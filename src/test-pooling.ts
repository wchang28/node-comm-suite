import { sleep } from "./utils.js";
import {Pooling} from "./pooling.js";

async function callback() {
    console.log("Hi");
    await sleep(3000);
    return 1;
}

async function test() {
    const pooling = new Pooling<number>(callback, 2000);

    pooling.on("started", () => {
        console.log(`[${new Date().toISOString()}] Pooling <<started>>`);
    }).on("stopping", () => {
        console.log(`[${new Date().toISOString()}] Pooling <<stopping>>`);
    }).on("stopped", () => {
        console.log(`[${new Date().toISOString()}] Pooling <<stopped>>`);
    }).on("calling-callback", () => {
        console.log(`[${new Date().toISOString()}] Calling callback...`);
    }).on("callback-returned", (result, error) => {
        console.log(`[${new Date().toISOString()}] Callback returned. Result:`, result, "Error:", error);
    }).on("sleeping", () => {
        console.log(`[${new Date().toISOString()}] <<sleeping>>...`);
    }).on("waking-up", () => {
        console.log(`[${new Date().toISOString()}] <<waking up>>...`);
    }).on("sleep-interrupted", () => {
        console.log(`[${new Date().toISOString()}] <<sleep interrupted>>`);
    }).start();
    await sleep(57000);
    //await sleep(60000);
    await pooling.stop();
}

test().then(() => {
    console.log(`[${new Date().toISOString()}] Test completed`);
}).catch((err) => {
    console.error(`[${new Date().toISOString()}] Test failed with error:`, err);
});