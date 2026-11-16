import {testBase64Encode, testBase64Decode} from "./jwt-test.js";

async function test() {
    /*
    const poolMssql = await testMssqlConnection();
    const pool = await odbc.pool({
        connectionString
        ,connectionTimeout: 60 // seconds
        ,loginTimeout: 10 // seconds
    });
    const result = await pool.query<{number: number}>('SELECT 1 AS [number] UNION ALL SELECT 2 AS [number]');
    console.log(result);
    const columns = result.columns
    for (const row of result) {
        const n = row["number"];
        const x = 1;
    }
    await pool.close();
    */
    const b64 = testBase64Encode();
    const s = testBase64Decode(b64);
    console.log(`${s}`);
    //await testPostgresConnection();
    const x= 1;
}

test().then(() => {
    console.log("done");
}).catch(err => {
    console.error("error:", err);
});