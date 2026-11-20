import {SequentialProcessor} from "./sequential-processor.js";
import {readFile, writeFile, rename} from "node:fs/promises";

const MIN_INT = 0;
const MAX_INT = 2147483647; // max INT type value for SQL

type Operation = "reset" | "set" | "next" | "current";
interface OperationRequest {
    operation: Operation;
    value?: number;
}

// A file-based sequence number generator that ensures sequential access to the sequence number.
export class FileBasedSequenceNumberGenerator {
    private readonly processor_: SequentialProcessor<OperationRequest, number>;
    // Reads the current value from the file.
    private async readCurrentValue() {
        let content: string;
        try {
            content = await readFile(this.filePath, "utf-8");
        } catch (e) {
            if ((e as NodeJS.ErrnoException).code === "ENOENT") return MIN_INT;
            throw e;
        }
        const trimmed = content.trim();
        if (!/^\d+$/.test(trimmed)) {
            throw new Error(`Corrupt sequence file ${this.filePath}: "${content}"`);
        }
        const currentValue = Number(trimmed);
        if (currentValue > MAX_INT) {
            throw new Error(`Sequence value out of range: ${currentValue}`);
        }
        return currentValue;
    }
    // Writes the current value to the file.
    private async writeCurrentValue(currentValue: number) {
        /*
        const tmp = `${this.filePath}.tmp`;
        await writeFile(tmp, currentValue.toString(), "utf-8");
        await rename(tmp, this.filePath);   // atomic replace
        */
        await writeFile(this.filePath, currentValue.toString(), "utf-8");
    }
    constructor(public filePath: string) {
        this.processor_ = new SequentialProcessor<OperationRequest, number>(async (input: OperationRequest) => {
            if (input.operation === "reset") {
                await this.writeCurrentValue(MIN_INT);
                return MIN_INT;
            }
            else if (input.operation === "set") {
                if (!Number.isInteger(input.value)) {
                    throw new Error(`Invalid value for set: ${input.value}`);
                }
                const value = Math.max(MIN_INT, Math.min(MAX_INT, input.value!));
                await this.writeCurrentValue(value);
                return value;
            }
            else if (input.operation === "next") {
                const currentValue = await this.readCurrentValue();
                const nextValue = (currentValue >= MAX_INT ? MIN_INT : currentValue + 1);
                await this.writeCurrentValue(nextValue);
                return nextValue;
            } else if (input.operation === "current") {
                return this.readCurrentValue();
            } else {
                throw new Error(`Invalid operation: ${input.operation}`);
            }
        });
    }
    // Returns the current value without modifying it.
    async current() {
        return this.processor_.submit({ operation: "current" });
    }
    // Returns the next value and increments the sequence.
    async next() {
        return this.processor_.submit({ operation: "next" });
    }
    // Resets the sequence to the minimum value.
    async reset() {
        return this.processor_.submit({ operation: "reset" });
    }
    // Sets the sequence to the specified value.
    async set(value: number) {
        return this.processor_.submit({ operation: "set", value });
    }
    // Returns whether the processor is idle.
    get idle(): boolean {
        return this.processor_.idle;
    }
    // Returns the number of pending operations in the processor.
    get pending(): number {
        return this.processor_.pending;
    }
}