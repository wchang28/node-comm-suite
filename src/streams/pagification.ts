import { Transform, TransformCallback } from 'node:stream';

/**
 * Groups incoming objects into arrays (pages) of up to `pageSize` items.
 * The last page may be smaller. Object mode on both sides.
 */
export class PagifyStream<T> extends Transform {
    private readonly _pageSize: number;
    private _buffer: T[] = [];

    constructor(pageSize: number) {
        if (!Number.isInteger(pageSize) || pageSize < 1) {
            throw new RangeError(`pageSize must be a positive integer, got ${pageSize}`);
        }
        super({ objectMode: true });
        this._pageSize = pageSize;
    }

    _transform(chunk: T, _encoding: BufferEncoding, callback: TransformCallback): void {
        this._buffer.push(chunk);
        if (this._buffer.length >= this._pageSize) {
            const page = this._buffer;
            this._buffer = [];
            this.push(page);
        }
        callback();
    }

    _flush(callback: TransformCallback): void {
        if (this._buffer.length > 0) {
            const page = this._buffer;
            this._buffer = [];
            this.push(page);
        }
        callback();
    }
}

/**
 * Inverse of PagifyStream: takes arrays (pages) and emits their items one at a time.
 * A non-array chunk is treated as a single-item page.
 *
 * `null` / `undefined` items are skipped, because pushing `null` would signal
 * end-of-stream and object-mode streams cannot carry them as data.
 */
export class UnpagifyStream<T> extends Transform {
    constructor() {
        super({ objectMode: true });
    }

    _transform(chunk: T[] | T, _encoding: BufferEncoding, callback: TransformCallback): void {
        const items = Array.isArray(chunk) ? chunk : [chunk];
        for (const item of items) {
            if (item !== null && item !== undefined) {
                this.push(item);
            }
        }
        callback();
    }
}
