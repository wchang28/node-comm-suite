import { EventEmitter } from 'events';

export interface SignalingEvents {
    "signaled": [];
}

export class Signaling extends EventEmitter {
    constructor() {
        super();
    }
    signal() {
        this.emit('signaled');
    }
    on<K extends keyof SignalingEvents>(
        event: K,
        listener: (...args: SignalingEvents[K]) => void
    ): this {
        return super.on(event, listener);
    }
    once<K extends keyof SignalingEvents>(
        event: K,
        listener: (...args: SignalingEvents[K]) => void
    ): this {
        return super.once(event, listener);
    }
    emit<K extends keyof SignalingEvents>(
        event: K,
        ...args: SignalingEvents[K]
    ): boolean {
        return super.emit(event, ...args);
    }
}