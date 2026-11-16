import { Transform, TransformCallback, TransformOptions } from 'node:stream';

export interface JsonStringifyStreamOptions
    extends Omit<TransformOptions, 'objectMode' | 'readableObjectMode' | 'writableObjectMode' | 'transform' | 'flush'> {
    /**
     * - 'array'  (default): emits a single JSON array: `[obj1,obj2,...]` (`[]` if no input).
     * - 'ndjson': emits newline-delimited JSON, one object per line.
     */
    format?: 'array' | 'ndjson';
    /** Passed to JSON.stringify as `space` for pretty printing (array format only). */
    space?: string | number;
    /** Passed to JSON.stringify as `replacer`. */
    replacer?: (this: any, key: string, value: any) => any;
}

/**
 * Writable side: object mode (accepts JS objects).
 * Readable side: text (UTF-8 JSON), so it can be piped to a file, HTTP response, etc.
 *
 * Objects are serialized one at a time as they arrive, so memory use stays flat
 * no matter how many objects pass through.
 */
export class JsonStringifyStream extends Transform {
    private readonly _format: 'array' | 'ndjson';
    private readonly _space?: string | number;
    private readonly _replacer?: (this: any, key: string, value: any) => any;
    private _first = true;

    constructor(options?: JsonStringifyStreamOptions) {
        const { format = 'array', space, replacer, ...streamOptions } = options ?? {};
        super({ ...streamOptions, writableObjectMode: true, readableObjectMode: false, encoding: 'utf8' });
        this._format = format;
        this._space = format === 'array' ? space : undefined;
        this._replacer = replacer;
    }

    _transform(chunk: unknown, _encoding: BufferEncoding, callback: TransformCallback): void {
        let json: string;
        try {
            // JSON.stringify returns undefined for undefined/functions/symbols; emit null like JSON arrays do.
            json = JSON.stringify(chunk, this._replacer, this._space) ?? 'null';
        } catch (err) {
            // e.g. circular references or BigInt values
            callback(err as Error);
            return;
        }

        if (this._format === 'ndjson') {
            callback(null, json + '\n');
            return;
        }

        if (this._space !== undefined) {
            json = indent(json, this._space);
        }
        const prefix = this._first ? '[' + this._newline() : ',' + this._newline();
        this._first = false;
        callback(null, prefix + json);
    }

    _flush(callback: TransformCallback): void {
        if (this._format === 'array') {
            callback(null, this._first ? '[]' : this._newline() + ']');
        } else {
            callback();
        }
    }

    private _newline(): string {
        return this._space !== undefined ? '\n' : '';
    }
}

/** Indents every line of `text` by one level of `space`, matching JSON.stringify's nesting. */
function indent(text: string, space: string | number): string {
    const pad = typeof space === 'number' ? ' '.repeat(Math.min(10, Math.max(0, space))) : space.slice(0, 10);
    return text.split('\n').map((line) => pad + line).join('\n');
}
