import { Transform, TransformCallback, TransformOptions } from 'node:stream';

export type AsyncObjectTransformer<I, O> = (input: I) => Promise<O>;

/** Returns true to keep the object, false to drop it. May be sync or async. */
export type ObjectFilter<I> = (input: I) => boolean | Promise<boolean>;

export interface ObjectTransformStreamOptions<I>
    extends Omit<TransformOptions, 'objectMode' | 'readableObjectMode' | 'writableObjectMode' | 'transform'> {
    /** Applied before the transformer; objects for which it returns false are skipped. */
    filter?: ObjectFilter<I>;
}

/**
 * A duplex (writable + readable) object-mode stream that maps each incoming
 * object to an outgoing object using an asynchronous transformer function.
 *
 * - An optional filter (sync or async) runs first; rejected objects are dropped
 *   and never reach the transformer.
 * - Objects are processed one at a time, so output order matches input order.
 * - Backpressure is honored: the next input isn't accepted until the current
 *   one has been filtered/transformed and its result pushed.
 * - If the filter or transformer throws/rejects, the stream is destroyed with that error.
 */
export class ObjectTransformStream<I, O> extends Transform {
    private readonly _transformer: AsyncObjectTransformer<I, O>;
    private readonly _filter?: ObjectFilter<I>;

    constructor(transformer: AsyncObjectTransformer<I, O>, options?: ObjectTransformStreamOptions<I>) {
        const { filter, ...streamOptions } = options ?? {};
        super({ ...streamOptions, objectMode: true });
        this._transformer = transformer;
        this._filter = filter;
    }

    _transform(chunk: I, _encoding: BufferEncoding, callback: TransformCallback): void {
        this._process(chunk).then(
            (result) => (result.keep ? callback(null, result.output) : callback()),
            (err) => callback(err instanceof Error ? err : new Error(String(err)))
        );
    }

    private async _process(chunk: I): Promise<{ keep: true; output: O } | { keep: false }> {
        if (this._filter && !(await this._filter(chunk))) {
            return { keep: false };
        }
        return { keep: true, output: await this._transformer(chunk) };
    }
}
