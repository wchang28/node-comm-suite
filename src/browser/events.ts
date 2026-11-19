/**
 * Browser implementation of Node's `node:events` EventEmitter, built on top of the
 * browser's native `EventTarget`.
 *
 * The public interface (instance methods, static methods, static properties, module
 * level exports, generic typings) mirrors `node:events` / `@types/node`, and the
 * runtime semantics follow Node's implementation:
 *   - listeners are invoked synchronously, in registration order, with `this` bound to the emitter
 *   - duplicate listeners are allowed (each registration is invoked)
 *   - event names can be strings or symbols
 *   - an exception thrown by a listener stops the remaining listeners and propagates out of `emit()`
 *   - `emit('error')` with no 'error' listener throws
 *   - 'newListener' / 'removeListener' meta events, `errorMonitor`, `once` wrappers (`rawListeners`)
 *   - max-listener leak warning (reported through `console.warn` instead of `process.emitWarning`)
 *   - `captureRejections` (uses `queueMicrotask` instead of `process.nextTick`)
 *   - static `once`, `on`, `getEventListeners`, `getMaxListeners`, `setMaxListeners`,
 *     `listenerCount`, `addAbortListener`
 *
 * Why not delegate everything to EventTarget? A raw EventTarget de-duplicates listeners,
 * only accepts string event types, and swallows listener exceptions (they are reported
 * to `window.onerror` rather than thrown from `dispatchEvent`). To get Node's exact
 * behavior, this class keeps an ordered listener registry and uses a private EventTarget
 * as the synchronous dispatch mechanism: each event name with at least one listener has
 * one native listener registered on the EventTarget, and `emit()` dispatches an Event that
 * carries the arguments and a snapshot of the listeners. Exceptions are captured inside
 * the native handler and re-thrown from `emit()`.
 *
 * Not provided (Node-only, no browser equivalent): `EventEmitterAsyncResource`, `usingDomains`,
 * and listing listeners of a raw (non-EventEmitter) EventTarget in `getEventListeners()`.
 */

// ---------------------------------------------------------------------------------------
// Typings (mirrors @types/node "events")
// ---------------------------------------------------------------------------------------

export type DefaultEventMap = [never];
export type EventMap<T> = Record<keyof T, any[]> | DefaultEventMap;
type AnyRest = [...args: any[]];
type Args<K, T> = T extends DefaultEventMap ? AnyRest : K extends keyof T ? T[K] : never;
type Key<K, T> = T extends DefaultEventMap ? string | symbol : K | keyof T;
type Key2<K, T> = T extends DefaultEventMap ? string | symbol : K & keyof T;
type Listener<K, T, F> = T extends DefaultEventMap
    ? F
    : K extends keyof T
        ? T[K] extends unknown[]
            ? (...args: T[K]) => void
            : never
        : never;
type Listener1<K, T> = Listener<K, T, (...args: any[]) => void>;
type Listener2<K, T> = Listener<K, T, Function>;

export interface EventEmitterOptions {
    /**
     * Enables automatic capturing of promise rejections returned by listeners.
     */
    captureRejections?: boolean | undefined;
}

export interface StaticEventEmitterOptions {
    signal?: AbortSignal | undefined;
}

export interface StaticEventEmitterIteratorOptions extends StaticEventEmitterOptions {
    /** Names of events that will end the iteration. */
    close?: string[] | undefined;
    /** Accepted for interface compatibility; ignored (stream back-pressure is Node specific). */
    highWaterMark?: number | undefined;
    /** Accepted for interface compatibility; ignored (stream back-pressure is Node specific). */
    lowWaterMark?: number | undefined;
}

export interface Abortable {
    signal?: AbortSignal | undefined;
}

/** Object returned by `addAbortListener()`; disposable via `Symbol.dispose` when available. */
export interface AbortListenerDisposable {
    [key: symbol]: () => void;
}

// ---------------------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------------------

type EventName = string | symbol;
type Fn = (...args: any[]) => unknown;
interface WrappedFn extends Fn {
    listener?: Fn;
}

interface Entry {
    type: string;           // the (unique) native EventTarget event type for this event name
    listeners: WrappedFn[]; // ordered registry, may contain duplicates and once-wrappers
}

/** Native Event dispatched on the wrapped EventTarget for each emit(). */
class EmitEvent extends Event {
    threw = false;
    error: unknown = undefined;
    constructor(type: string, readonly eventName: EventName, readonly handlers: WrappedFn[], readonly args: unknown[]) {
        super(type);
    }
}

const errorMonitor: unique symbol = Symbol('events.errorMonitor');
const captureRejectionSymbol: unique symbol = Symbol.for('nodejs.rejection');
const disposeSymbol: symbol = (Symbol as unknown as { dispose?: symbol }).dispose ?? Symbol.for('nodejs.dispose');

function describeReceived(value: unknown): string {
    if (value == null) return ` Received ${value}`;
    if (typeof value === 'function') return ` Received function ${(value as Function).name || '<anonymous>'}`;
    if (typeof value === 'object') {
        const ctorName = (value as object).constructor?.name;
        return ctorName ? ` Received an instance of ${ctorName}` : ` Received ${safeInspect(value)}`;
    }
    let shown = safeInspect(value);
    if (shown.length > 28) shown = `${shown.slice(0, 25)}...`;
    return ` Received type ${typeof value} (${shown})`;
}

/** Small subset of Node's `util.inspect()` used to format values in error messages. */
function safeInspect(value: unknown, depth: number = 2, seen: Set<object> = new Set()): string {
    switch (typeof value) {
        case 'string': return `'${value}'`;
        case 'bigint': return `${value}n`;
        case 'symbol': return value.toString();
        case 'function': return `[Function: ${value.name || '(anonymous)'}]`;
        case 'object': break;
        default: return String(value); // number, boolean, undefined
    }
    if (value === null) return 'null';
    const obj = value as Record<string, unknown>;
    if (seen.has(obj)) return '[Circular *1]';
    if (obj instanceof Error) return obj.stack ?? `${obj.name}: ${obj.message}`;
    if (obj instanceof Date) return obj.toISOString();
    if (obj instanceof RegExp) return obj.toString();
    const isArray = Array.isArray(obj);
    const ctorName = obj.constructor?.name;
    const prefix = isArray
        ? (ctorName && ctorName !== 'Array' ? `${ctorName}(${(obj as unknown as unknown[]).length}) ` : '')
        : (ctorName === undefined ? '[Object: null prototype] ' : ctorName !== 'Object' ? `${ctorName} ` : '');
    const keys = Object.keys(obj);
    if (keys.length === 0) return `${prefix}${isArray ? '[]' : '{}'}`;
    if (depth < 0) return isArray ? '[Array]' : `[${ctorName || 'Object'}]`;
    seen.add(obj);
    const parts = keys.map((k) => {
        const v = safeInspect(obj[k], depth - 1, seen);
        return isArray && isArrayIndexKey(k) ? v : `${/^[A-Za-z_$][\w$]*$/.test(k) ? k : `'${k}'`}: ${v}`;
    });
    seen.delete(obj);
    return isArray ? `${prefix}[ ${parts.join(', ')} ]` : `${prefix}{ ${parts.join(', ')} }`;
}

function codedError<E extends Error>(err: E, code: string): E {
    Object.defineProperty(err, 'code', { value: code, enumerable: false, writable: true, configurable: true });
    return err;
}

function invalidArgType(name: string, expected: string, actual: unknown, isProperty = false): TypeError {
    const what = isProperty ? 'property' : 'argument';
    return codedError(
        new TypeError(`The "${name}" ${what} must be ${expected}.${describeReceived(actual)}`),
        'ERR_INVALID_ARG_TYPE'
    );
}

function checkListener(listener: unknown): asserts listener is Fn {
    if (typeof listener !== 'function') {
        throw invalidArgType('listener', 'of type function', listener);
    }
}

function validateNonNegativeNumber(n: unknown, name: string): asserts n is number {
    if (typeof n !== 'number') {
        throw invalidArgType(name, 'of type number', n);
    }
    if (n < 0 || Number.isNaN(n)) {
        throw codedError(
            new RangeError(`The value of "${name}" is out of range. It must be >= 0. Received ${safeInspect(n)}`),
            'ERR_OUT_OF_RANGE'
        );
    }
}

function validateBoolean(value: unknown, name: string): asserts value is boolean {
    if (typeof value !== 'boolean') {
        throw invalidArgType(name, 'of type boolean', value, name.includes('.'));
    }
}

function validateAbortSignal(signal: unknown, name: string): asserts signal is AbortSignal | undefined {
    if (signal !== undefined && (signal === null || typeof signal !== 'object' || !('aborted' in signal))) {
        throw invalidArgType(name, 'an instance of AbortSignal', signal, name.includes('.'));
    }
}

function isArrayIndexKey(key: string): boolean {
    return /^(?:0|[1-9]\d*)$/.test(key) && Number(key) < 4294967295;
}

function isEventTarget(obj: unknown): obj is EventTarget {
    return typeof EventTarget !== 'undefined' && obj instanceof EventTarget;
}

/** Error thrown when an operation is aborted through an AbortSignal (same shape as Node's AbortError). */
export class AbortError extends Error {
    readonly code = 'ABORT_ERR';
    constructor(message: string = 'The operation was aborted', options?: { cause?: unknown }) {
        super(message, options);
        this.name = 'AbortError';
    }
}

// ---------------------------------------------------------------------------------------
// EventEmitter
// ---------------------------------------------------------------------------------------

export class EventEmitter<T extends EventMap<T> = DefaultEventMap> {
    // ----- static members -----

    /** Self reference, so that `import { EventEmitter } from ...` and `EventEmitter.EventEmitter` both work. */
    static declare readonly EventEmitter: typeof EventEmitter; // assigned after the class body
    /** Symbol for watching 'error' events without consuming them. */
    static readonly errorMonitor: typeof errorMonitor = errorMonitor;
    /** Symbol for a custom rejection handler (used with `captureRejections`). */
    static readonly captureRejectionSymbol: typeof captureRejectionSymbol = captureRejectionSymbol;

    static #defaultMaxListeners = 10;
    static #captureRejectionsDefault = false;
    static #eventTargetMaxListeners = new WeakMap<EventTarget, number>();
    static #typeCounter = 0;

    /** Default max listeners for all new EventEmitter instances. */
    static get defaultMaxListeners(): number {
        return EventEmitter.#defaultMaxListeners;
    }
    static set defaultMaxListeners(n: number) {
        validateNonNegativeNumber(n, 'defaultMaxListeners');
        EventEmitter.#defaultMaxListeners = n;
    }

    /** Default value of the `captureRejections` option for new instances. */
    static get captureRejections(): boolean {
        return EventEmitter.#captureRejectionsDefault;
    }
    static set captureRejections(value: boolean) {
        validateBoolean(value, 'EventEmitter.captureRejections');
        EventEmitter.#captureRejectionsDefault = value;
    }

    /**
     * Creates a Promise that is fulfilled when the emitter emits the given event, or rejected if
     * the emitter emits 'error' while waiting. Resolves with an array of the event arguments.
     */
    static once(
        emitter: EventEmitter<any> | EventTarget,
        eventName: string | symbol,
        options?: StaticEventEmitterOptions
    ): Promise<any[]> {
        const signal = options?.signal;
        validateAbortSignal(signal, 'options.signal');
        if (signal?.aborted) {
            return Promise.reject(new AbortError(undefined, { cause: signal.reason }));
        }
        return new Promise<any[]>((resolve, reject) => {
            const ee = emitter as EventEmitter<any>;
            const isEE = typeof (emitter as any).on === 'function';
            const errorListener = (err: unknown) => {
                ee.removeListener(eventName, resolver);
                if (signal) signal.removeEventListener('abort', abortListener);
                reject(err);
            };
            const resolver = (...args: any[]) => {
                if (isEE) ee.removeListener('error', errorListener);
                if (signal) signal.removeEventListener('abort', abortListener);
                resolve(args);
            };
            const abortListener = () => {
                if (isEE) {
                    ee.removeListener(eventName, resolver);
                    ee.removeListener('error', errorListener);
                } else {
                    (emitter as EventTarget).removeEventListener(eventName as string, resolver);
                }
                reject(new AbortError(undefined, { cause: signal?.reason }));
            };
            if (isEE) {
                ee.once(eventName, resolver);
                if (eventName !== 'error') ee.once('error', errorListener);
            } else if (isEventTarget(emitter)) {
                emitter.addEventListener(eventName as string, resolver, { once: true });
            } else {
                throw invalidArgType('emitter', 'an instance of EventEmitter or EventTarget', emitter);
            }
            if (signal) signal.addEventListener('abort', abortListener, { once: true });
        });
    }

    /**
     * Returns an AsyncIterator that iterates over the given event. Each value is the array of
     * event arguments. An 'error' event makes the iterator throw; iteration ends on `return()`,
     * on abort of `options.signal`, or when one of the `options.close` events is emitted.
     */
    static on(
        emitter: EventEmitter<any> | EventTarget,
        eventName: string | symbol,
        options?: StaticEventEmitterIteratorOptions
    ): AsyncIterableIterator<any[]> {
        const signal = options?.signal;
        validateAbortSignal(signal, 'options.signal');
        if (signal?.aborted) {
            throw new AbortError(undefined, { cause: signal.reason });
        }
        const isEE = typeof (emitter as any).on === 'function';
        if (!isEE && !isEventTarget(emitter)) {
            throw invalidArgType('emitter', 'an instance of EventEmitter or EventTarget', emitter);
        }

        const unconsumedEvents: any[][] = [];
        const unconsumedPromises: { resolve: (r: IteratorResult<any[]>) => void; reject: (e: unknown) => void }[] = [];
        let error: unknown = null;
        let finished = false;
        const cleanups: Array<() => void> = [];

        const listen = (name: string | symbol, handler: Fn) => {
            if (isEE) {
                (emitter as EventEmitter<any>).on(name, handler);
                cleanups.push(() => (emitter as EventEmitter<any>).removeListener(name, handler));
            } else {
                (emitter as EventTarget).addEventListener(name as string, handler);
                cleanups.push(() => (emitter as EventTarget).removeEventListener(name as string, handler));
            }
        };

        const closeHandler = (): Promise<IteratorResult<any[]>> => {
            for (const cleanup of cleanups.splice(0)) cleanup();
            if (signal) signal.removeEventListener('abort', abortListener);
            finished = true;
            const doneResult: IteratorResult<any[]> = { value: undefined, done: true };
            for (const p of unconsumedPromises.splice(0)) p.resolve(doneResult);
            return Promise.resolve(doneResult);
        };

        const errorHandler = (err: unknown) => {
            const pending = unconsumedPromises.shift();
            if (pending) pending.reject(err);
            else error = err;
            void closeHandler();
        };

        const abortListener = () => {
            errorHandler(new AbortError(undefined, { cause: signal?.reason }));
        };

        const eventHandler = (...args: any[]) => {
            if (finished) return;
            const pending = unconsumedPromises.shift();
            if (pending) pending.resolve({ value: args, done: false });
            else unconsumedEvents.push(args);
        };

        listen(eventName, eventHandler);
        if (eventName !== 'error' && isEE) listen('error', errorHandler);
        for (const name of options?.close ?? []) listen(name, () => void closeHandler());
        if (signal) signal.addEventListener('abort', abortListener, { once: true });

        const iterator: AsyncIterableIterator<any[]> = {
            next(): Promise<IteratorResult<any[]>> {
                const value = unconsumedEvents.shift();
                if (value) return Promise.resolve({ value, done: false });
                if (error) {
                    const p = Promise.reject(error);
                    error = null;
                    return p;
                }
                if (finished) return closeHandler();
                return new Promise((resolve, reject) => unconsumedPromises.push({ resolve, reject }));
            },
            return(): Promise<IteratorResult<any[]>> {
                return closeHandler();
            },
            throw(err?: unknown): Promise<IteratorResult<any[]>> {
                if (!err || !(err instanceof Error)) {
                    throw invalidArgType('EventEmitter.AsyncIterator', 'an instance of Error', err);
                }
                errorHandler(err);
                return Promise.resolve({ value: undefined, done: true });
            },
            [Symbol.asyncIterator]() {
                return this;
            },
        };
        return iterator;
    }

    /**
     * Returns a copy of the array of listeners for the event named `eventName`.
     * Only EventEmitter instances are supported in the browser: a raw EventTarget provides no
     * way to enumerate its listeners.
     */
    static getEventListeners(emitter: EventEmitter<any> | EventTarget, eventName: string | symbol): Function[] {
        if (typeof (emitter as any)?.listeners === 'function') {
            return (emitter as EventEmitter<any>).listeners(eventName);
        }
        if (isEventTarget(emitter)) {
            throw new TypeError('getEventListeners() is not supported for a native EventTarget in the browser');
        }
        throw invalidArgType('emitter', 'an instance of EventEmitter or EventTarget', emitter);
    }

    /** Returns the currently set max amount of listeners. */
    static getMaxListeners(emitter: EventEmitter<any> | EventTarget): number {
        if (typeof (emitter as any)?.getMaxListeners === 'function') {
            return (emitter as EventEmitter<any>).getMaxListeners();
        }
        if (isEventTarget(emitter)) {
            return EventEmitter.#eventTargetMaxListeners.get(emitter) ?? EventEmitter.#defaultMaxListeners;
        }
        throw invalidArgType('emitter', 'an instance of EventEmitter or EventTarget', emitter);
    }

    /**
     * Sets max listeners on the given emitters/targets. With no targets, sets
     * `EventEmitter.defaultMaxListeners`.
     */
    static setMaxListeners(n: number = EventEmitter.#defaultMaxListeners, ...eventTargets: Array<EventEmitter<any> | EventTarget>): void {
        validateNonNegativeNumber(n, 'setMaxListeners');
        if (eventTargets.length === 0) {
            EventEmitter.#defaultMaxListeners = n;
            return;
        }
        for (const target of eventTargets) {
            if (isEventTarget(target)) {
                EventEmitter.#eventTargetMaxListeners.set(target, n);
            } else if (typeof (target as any)?.setMaxListeners === 'function') {
                (target as EventEmitter<any>).setMaxListeners(n);
            } else {
                throw invalidArgType('eventTargets', 'an instance of EventEmitter or EventTarget', target);
            }
        }
    }

    /** @deprecated Use `emitter.listenerCount()` instead. */
    static listenerCount(emitter: EventEmitter<any>, eventName: string | symbol): number {
        return emitter.listenerCount(eventName);
    }

    /**
     * Listens once to the `abort` event on the provided signal, even if `stopImmediatePropagation`
     * is called on the abort event by another listener. Returns a disposable that removes the listener.
     */
    static addAbortListener(signal: AbortSignal, listener: (event: Event) => void): AbortListenerDisposable {
        if (signal === undefined) throw invalidArgType('signal', 'an instance of AbortSignal', signal);
        validateAbortSignal(signal, 'signal');
        if (typeof listener !== 'function') throw invalidArgType('listener', 'of type function', listener);
        let removeEventListener: (() => void) | undefined;
        if (signal.aborted) {
            queueMicrotask(() => (listener as Fn)());
        } else {
            signal.addEventListener('abort', listener, { once: true });
            removeEventListener = () => signal.removeEventListener('abort', listener);
        }
        return {
            [disposeSymbol]() {
                removeEventListener?.();
            },
        };
    }

    // ----- instance members -----

    #target: EventTarget = new EventTarget();
    #events = new Map<EventName, Entry>();
    #warned = new Set<EventName>();
    #maxListeners: number | undefined = undefined;
    #captureRejections: boolean;

    /** Optional custom handler for rejections captured when `captureRejections` is enabled. */
    [captureRejectionSymbol]?<K>(error: Error, event: Key<K, T>, ...args: Args<K, T>): void;

    constructor(options?: EventEmitterOptions) {
        const capture = options?.captureRejections;
        if (capture !== undefined) {
            validateBoolean(capture, 'options.captureRejections');
            this.#captureRejections = capture;
        } else {
            this.#captureRejections = EventEmitter.#captureRejectionsDefault;
        }
    }

    /** Alias for `emitter.on(eventName, listener)`. */
    addListener<K>(eventName: Key<K, T>, listener: Listener1<K, T>): this {
        return this.#addListener(eventName as EventName, listener as unknown as Fn, false);
    }

    /** Adds the listener to the end of the listeners array for the event named `eventName`. */
    on<K>(eventName: Key<K, T>, listener: Listener1<K, T>): this {
        return this.#addListener(eventName as EventName, listener as unknown as Fn, false);
    }

    /** Adds a one-time listener for the event named `eventName`. */
    once<K>(eventName: Key<K, T>, listener: Listener1<K, T>): this {
        checkListener(listener);
        const wrapped = this.#onceWrap(eventName as EventName, listener as unknown as Fn);
        return (this.on as unknown as (n: EventName, l: Fn) => this).call(this, eventName as EventName, wrapped);
    }

    /** Adds the listener to the beginning of the listeners array for the event named `eventName`. */
    prependListener<K>(eventName: Key<K, T>, listener: Listener1<K, T>): this {
        return this.#addListener(eventName as EventName, listener as unknown as Fn, true);
    }

    /** Adds a one-time listener to the beginning of the listeners array. */
    prependOnceListener<K>(eventName: Key<K, T>, listener: Listener1<K, T>): this {
        checkListener(listener);
        const wrapped = this.#onceWrap(eventName as EventName, listener as unknown as Fn);
        return (this.prependListener as unknown as (n: EventName, l: Fn) => this).call(this, eventName as EventName, wrapped);
    }

    /** Removes the most recently added instance of `listener` from the listeners array for `eventName`. */
    removeListener<K>(eventName: Key<K, T>, listener: Listener1<K, T>): this {
        checkListener(listener);
        const name = eventName as EventName;
        const entry = this.#events.get(name);
        if (entry === undefined) return this;
        const list = entry.listeners;
        for (let i = list.length - 1; i >= 0; i--) {
            const registered = list[i];
            if (registered === listener || registered.listener === listener) {
                const wasSingle = list.length === 1;
                list.splice(i, 1);
                if (list.length === 0) {
                    this.#deleteEntry(name, entry);
                } else if (list.length === 1) {
                    this.#warned.delete(name);
                }
                if (this.#events.has('removeListener')) {
                    // Same argument Node passes: the unwrapped listener when the event had a single
                    // listener, otherwise the listener argument as given.
                    this.#emitInternal('removeListener', name, wasSingle ? (registered.listener ?? listener) : listener);
                }
                break;
            }
        }
        return this;
    }

    /** Alias for `emitter.removeListener()`. */
    off<K>(eventName: Key<K, T>, listener: Listener1<K, T>): this {
        return (this.removeListener as unknown as (n: EventName, l: Fn) => this).call(this, eventName as EventName, listener as unknown as Fn);
    }

    /** Removes all listeners, or those of the specified `eventName`. */
    removeAllListeners(eventName?: Key<unknown, T>): this {
        const hasName = eventName !== undefined;
        const name = eventName as EventName;
        const removeOne = this.removeListener as unknown as (n: EventName, l: Fn) => this;

        // Not listening for 'removeListener', no need to emit.
        if (!this.#events.has('removeListener')) {
            if (!hasName) {
                for (const [key, entry] of [...this.#events]) this.#deleteEntry(key, entry);
            } else {
                const entry = this.#events.get(name);
                if (entry !== undefined) this.#deleteEntry(name, entry);
            }
            return this;
        }

        // Emit 'removeListener' for all listeners on all events.
        if (!hasName) {
            for (const key of this.eventNames()) {
                if (key === 'removeListener') continue;
                (this.removeAllListeners as (n: EventName) => this).call(this, key);
            }
            (this.removeAllListeners as (n: EventName) => this).call(this, 'removeListener');
            for (const [key, entry] of [...this.#events]) this.#deleteEntry(key, entry);
            return this;
        }

        const entry = this.#events.get(name);
        if (entry !== undefined) {
            const list = entry.listeners.slice();
            // LIFO order
            for (let i = list.length - 1; i >= 0; i--) removeOne.call(this, name, list[i]);
        }
        return this;
    }

    /** Sets the max number of listeners per event before a leak warning is printed. 0/Infinity = unlimited. */
    setMaxListeners(n: number): this {
        validateNonNegativeNumber(n, 'setMaxListeners');
        this.#maxListeners = n;
        return this;
    }

    /** Returns the current max listener value. */
    getMaxListeners(): number {
        return this.#maxListeners ?? EventEmitter.#defaultMaxListeners;
    }

    /** Returns a copy of the listeners for `eventName` (once-wrappers are unwrapped). */
    listeners<K>(eventName: Key<K, T>): Array<Listener2<K, T>> {
        const entry = this.#events.get(eventName as EventName);
        if (entry === undefined) return [];
        return entry.listeners.map((l) => l.listener ?? l) as unknown as Array<Listener2<K, T>>;
    }

    /** Returns a copy of the listeners for `eventName`, including once-wrappers (which expose `.listener`). */
    rawListeners<K>(eventName: Key<K, T>): Array<Listener2<K, T>> {
        const entry = this.#events.get(eventName as EventName);
        if (entry === undefined) return [];
        return entry.listeners.slice() as unknown as Array<Listener2<K, T>>;
    }

    /**
     * Synchronously calls each listener registered for `eventName`, in registration order,
     * passing the supplied arguments. Returns `true` if the event had listeners, `false` otherwise.
     */
    emit<K>(eventName: Key<K, T>, ...args: Args<K, T>): boolean {
        const name = eventName as EventName;
        let doError = name === 'error';

        if (doError) {
            if (this.#events.has(errorMonitor)) {
                this.#emitInternal(errorMonitor, ...(args as unknown[]));
            }
            doError = !this.#events.has('error');
        }

        // If there is no 'error' event listener then throw.
        if (doError) {
            const er = (args as unknown[]).length > 0 ? (args as unknown[])[0] : undefined;
            if (er instanceof Error) {
                throw er; // Unhandled 'error' event
            }
            const err = codedError(
                new Error(`Unhandled error. (${safeInspect(er)})`),
                'ERR_UNHANDLED_ERROR'
            );
            (err as Error & { context?: unknown }).context = er;
            throw err; // Unhandled 'error' event
        }

        const entry = this.#events.get(name);
        if (entry === undefined) return false;

        // Snapshot the listeners, so add/remove during emit does not affect this emit (same as Node).
        const event = new EmitEvent(entry.type, name, entry.listeners.slice(), args as unknown[]);
        this.#target.dispatchEvent(event);
        if (event.threw) throw event.error;
        return true;
    }

    /** Returns the number of listeners for `eventName`; if `listener` is given, counts only that listener. */
    listenerCount<K>(eventName: Key<K, T>, listener?: Listener2<K, T>): number {
        const entry = this.#events.get(eventName as EventName);
        if (entry === undefined) return 0;
        if (listener != null) {
            let matching = 0;
            for (const l of entry.listeners) {
                if (l === listener || l.listener === listener) matching++;
            }
            return matching;
        }
        return entry.listeners.length;
    }

    /** Returns the event names for which the emitter has registered listeners (same order as Node). */
    eventNames(): Array<(string | symbol) & Key2<unknown, T>> {
        // Node returns Reflect.ownKeys(_events): integer-like keys ascending, then other strings
        // in insertion order, then symbols in insertion order.
        const indexKeys: string[] = [];
        const stringKeys: string[] = [];
        const symbolKeys: symbol[] = [];
        for (const key of this.#events.keys()) {
            if (typeof key === 'symbol') symbolKeys.push(key);
            else if (isArrayIndexKey(key)) indexKeys.push(key);
            else stringKeys.push(key);
        }
        indexKeys.sort((a, b) => Number(a) - Number(b));
        return [...indexKeys, ...stringKeys, ...symbolKeys] as Array<(string | symbol) & Key2<unknown, T>>;
    }

    // ----- private helpers -----

    /** Native EventTarget listener: invokes the snapshot of listeners carried by the event. */
    #nativeHandler = (ev: Event): void => {
        const event = ev as EmitEvent;
        for (const handler of event.handlers) {
            try {
                const result = Reflect.apply(handler, this, event.args);
                if (result !== undefined && result !== null && this.#captureRejections) {
                    this.#addCatch(result, event.eventName, event.args);
                }
            } catch (err) {
                // EventTarget would swallow the exception; capture it so emit() can re-throw it,
                // and stop calling the remaining listeners (same as Node).
                event.threw = true;
                event.error = err;
                return;
            }
        }
    };

    #addListener(eventName: EventName, listener: WrappedFn, prepend: boolean): this {
        checkListener(listener);

        // Emit 'newListener' before adding, so a 'newListener' handler can add listeners that run first.
        if (this.#events.has('newListener')) {
            this.#emitInternal('newListener', eventName, listener.listener ?? listener);
        }

        let entry = this.#events.get(eventName);
        if (entry === undefined) {
            entry = { type: `ee:${EventEmitter.#typeCounter++}`, listeners: [] };
            this.#events.set(eventName, entry);
            this.#target.addEventListener(entry.type, this.#nativeHandler);
        }
        if (prepend) entry.listeners.unshift(listener);
        else entry.listeners.push(listener);

        // Check for listener leak
        const m = this.getMaxListeners();
        if (m > 0 && entry.listeners.length > m && !this.#warned.has(eventName)) {
            this.#warned.add(eventName);
            const w = new Error(
                'Possible EventEmitter memory leak detected. ' +
                `${entry.listeners.length} ${String(eventName)} listeners ` +
                `added to [${this.constructor.name}]. MaxListeners is ${m}. ` +
                'Use emitter.setMaxListeners() to increase limit'
            ) as Error & { emitter?: unknown; type?: EventName; count?: number };
            w.name = 'MaxListenersExceededWarning';
            w.emitter = this;
            w.type = eventName;
            w.count = entry.listeners.length;
            console.warn(w);
        }
        return this;
    }

    #deleteEntry(eventName: EventName, entry: Entry): void {
        this.#target.removeEventListener(entry.type, this.#nativeHandler);
        this.#events.delete(eventName);
        this.#warned.delete(eventName);
    }

    #onceWrap(eventName: EventName, listener: Fn): WrappedFn {
        const target = this;
        let fired = false;
        const wrapFn: WrappedFn = function (...args: unknown[]) {
            if (!fired) {
                (target.removeListener as unknown as (n: EventName, l: Fn) => unknown).call(target, eventName, wrapFn);
                fired = true;
                return Reflect.apply(listener, target, args);
            }
            return undefined;
        };
        Object.defineProperty(wrapFn, 'name', { value: 'bound onceWrapper', configurable: true });
        wrapFn.listener = listener;
        return wrapFn;
    }

    /** Calls `this.emit` (so subclass overrides are honored, as in Node) without generic type constraints. */
    #emitInternal(eventName: EventName, ...args: unknown[]): boolean {
        return (this.emit as unknown as (n: EventName, ...a: unknown[]) => boolean).call(this, eventName, ...args);
    }

    #addCatch(promise: unknown, eventName: EventName, args: unknown[]): void {
        try {
            const then = (promise as { then?: unknown }).then;
            if (typeof then === 'function') {
                then.call(promise, undefined, (err: unknown) => {
                    queueMicrotask(() => this.#emitUnhandledRejectionOrErr(err, eventName, args));
                });
            }
        } catch (err) {
            this.#emitInternal('error', err);
        }
    }

    #emitUnhandledRejectionOrErr(err: unknown, eventName: EventName, args: unknown[]): void {
        const handler = (this as unknown as Record<symbol, unknown>)[captureRejectionSymbol];
        if (typeof handler === 'function') {
            handler.call(this, err, eventName, ...args);
        } else {
            const prev = this.#captureRejections;
            try {
                this.#captureRejections = false;
                this.#emitInternal('error', err);
            } finally {
                this.#captureRejections = prev;
            }
        }
    }
}

Object.defineProperty(EventEmitter, 'EventEmitter', { value: EventEmitter, enumerable: true });

// ---------------------------------------------------------------------------------------
// Module level exports (same shape as `node:events`)
// ---------------------------------------------------------------------------------------

export const once = EventEmitter.once;
export const on = EventEmitter.on;
export const getEventListeners = EventEmitter.getEventListeners;
export const getMaxListeners = EventEmitter.getMaxListeners;
export const setMaxListeners = EventEmitter.setMaxListeners;
export const listenerCount = EventEmitter.listenerCount;
export const addAbortListener = EventEmitter.addAbortListener;
export { errorMonitor, captureRejectionSymbol };

export default EventEmitter;
