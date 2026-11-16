export function testBase64Encode(): string {
    const o ={"typ":"JWT","alg":"RS256","x5t":"rtsFT-b-7LuY7DVYeSNKcIJ7Vnc","kid":"rtsFT-b-7LuY7DVYeSNKcIJ7Vnc"};
    const json = JSON.stringify(o);
    const buff = Buffer.from(json, 'utf-8');
    const b64 = buff.toString('base64');
    return b64;
}
export function testBase64Decode(base64S: string): string {
    const buff = Buffer.from(base64S, 'base64')
    const s = buff.toString('utf-8');
    return s;
}