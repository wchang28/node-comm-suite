import { EventEmitter } from 'events';
import {Signaling} from './signaling.js';
import assert from 'assert';

export interface PoolingEvents {
    "started": [];
    "stopping": [];
    "stopped": [];
    "calling-callback": [];
    "callback-returned": [result: any, error: any];
    "sleeping": [];
    "waking-up": [];
    "sleep-interrupted": [];
}

export class Pooling<T> extends EventEmitter {
    private running: boolean = false;
    private _looping: boolean = false;
    private _exitSleepSignaling: Signaling = new Signaling();
    private _loopExitSignaling: Signaling = new Signaling();

    constructor(private readonly callback: () => Promise<T>, private readonly intervalMs: number = 1000) {
        super();
        assert(intervalMs > 0, "Interval must be greater than 0");
    }

    start(): void {
        if (this.running) {
            return;
        }
        this.running = true;
        this.loop();
        this.emit("started");
    }

    async stop(): Promise<void> {
        if (this.running) {
            this.emit("stopping");
            this.running = false;
            if (this._looping) {
                const me = this;
                const loopExitPromise = new Promise<void>((resolve) => {
                    me._loopExitSignaling.once('signaled', () => {
                        resolve();
                    });
                });
                this._exitSleepSignaling.signal();
                await loopExitPromise;
                this._loopExitSignaling.removeAllListeners();
                assert(!this._looping, "Pooling loop should have exited but is still looping");
            }
            this.emit("stopped");
        }
    }

    isRunning(): boolean {
        return this.running;
    }   

    private get sleepPromise(): Promise<void> {
        const me = this;
        return new Promise((resolve) => {
            const sleepTimer = setTimeout(() => {
                resolve();
            }, me.intervalMs);
            me._exitSleepSignaling.once('signaled', () => {
                clearTimeout(sleepTimer);
                me.emit("sleep-interrupted");
                resolve();
            });
        });
    }
    private async sleep() {
        await this.sleepPromise;
        this._exitSleepSignaling.removeAllListeners();
    }

    private async loop(): Promise<void> {
        this._looping = true;
        while (this.running) {
            this.emit("calling-callback");
            try {
                const result = await this.callback();
                this.emit("callback-returned", result, null);
            } catch (error) {
                this.emit("callback-returned", null, error);
            }
            if (this.running) {
                this.emit("sleeping");
                await this.sleep();
                this.emit("waking-up");
            }
        }
        this._loopExitSignaling.signal();
        this._looping = false;
    }

    on<K extends keyof PoolingEvents>(
        event: K,
        listener: (...args: PoolingEvents[K]) => void
    ): this {
        return super.on(event, listener);
    }
    once<K extends keyof PoolingEvents>(
        event: K,
        listener: (...args: PoolingEvents[K]) => void
    ): this {
        return super.once(event, listener);
    }
    emit<K extends keyof PoolingEvents>(
        event: K,
        ...args: PoolingEvents[K]
    ): boolean {
        return super.emit(event, ...args);
    }
}