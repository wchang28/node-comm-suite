import {SequentialProcessor, JobSummaryRow} from "./sequential-processor.js";
import {readFile, writeFile, rename} from "node:fs/promises";

const MIN_INT = 0;
const MAX_INT = 2147483647; // max INT type value for SQL

type Operation = "reset" | "set" | "next" | "current";
interface OperationRequest {
    operation: Operation;
    value?: number;
}

// Reads the current value from the file.
async function readCurrentValue(filePath: string) {
    let content: string;
    try {
        content = await readFile(filePath, "utf-8");
    } catch (e) {
        if ((e as NodeJS.ErrnoException).code === "ENOENT") return MIN_INT;
        throw e;
    }
    const trimmed = content.trim();
    if (!/^\d+$/.test(trimmed)) {
        throw new Error(`Corrupt sequence file ${filePath}: "${content}"`);
    }
    const currentValue = Number(trimmed);
    if (currentValue > MAX_INT) {
        throw new Error(`Sequence value out of range: ${currentValue}`);
    }
    return currentValue;
}

// Writes the current value to the file.
async function writeCurrentValue(filePath: string, currentValue: number) {
    /*
    const tmp = `${this.filePath}.tmp`;
    await writeFile(tmp, currentValue.toString(), "utf-8");
    await rename(tmp, this.filePath);   // atomic replace
    */
    await writeFile(filePath, currentValue.toString(), "utf-8");
}

// A file-based sequence number generator that ensures sequential access to the sequence number.
export class FileBasedSequenceNumberGenerator extends SequentialProcessor<OperationRequest, number> {
    constructor(filePath: string) {
        super(async (input: OperationRequest) => {
            if (input.operation === "reset") {
                await writeCurrentValue(filePath, MIN_INT);
                return MIN_INT;
            }
            else if (input.operation === "set") {
                if (!Number.isInteger(input.value)) {
                    throw new Error(`Invalid value for set: ${input.value}`);
                }
                const value = Math.max(MIN_INT, Math.min(MAX_INT, input.value!));
                await writeCurrentValue(filePath, value);
                return value;
            }
            else if (input.operation === "next") {
                const currentValue = await readCurrentValue(filePath);
                const nextValue = (currentValue >= MAX_INT ? MIN_INT : currentValue + 1);
                await writeCurrentValue(filePath, nextValue);
                return nextValue;
            } else if (input.operation === "current") {
                return readCurrentValue(filePath);
            } else {
                throw new Error(`Invalid operation: ${input.operation}`);
            }
        });
    }
    // Returns the current value without modifying it.
    async current() {
        return this.submit({ operation: "current" });
    }
    // Returns the next value and increments the sequence.
    async next() {
        return this.submit({ operation: "next" });
    }
    // Resets the sequence to the minimum value.
    async reset() {
        return this.submit({ operation: "reset" });
    }
    // Sets the sequence to the specified value.
    async set(value: number) {
        return this.submit({ operation: "set", value });
    }
}