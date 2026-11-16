import { Writable, WritableOptions, Readable, ReadableOptions } from 'node:stream';

export function makeStringReadable(s: string, options?: ReadableOptions): Readable {
    return Readable.from([s], options);
}

export class StringWritable extends Writable {
    private _chunks: string[];
    constructor(options?: WritableOptions) {
        super(options);
        this._chunks = [];
    }
    // Captures individual data chunks as they are written to the stream
    _write(chunk: any, encoding: string, callback: (error?: Error | null) => void) {
        // Chunks can arrive as Buffers, so convert them to a string
        this._chunks.push(chunk.toString());
        callback();
    }
    // Helper method to retrieve the fully aggregated text
    get text(): string {
        return this._chunks.join('');
    }
}