import {SequentialProcessor} from "./sequential-processor.js";
import fs from "node:fs";

const MIN_INT = 0;
const MAX_INT = 2147483647; // max INT type value for SQL

type Operation = "reset" | "set" | "next";
interface OperationRequest {
    operation: Operation;
    value?: number;
}

export class FileBasedSequenceNumberGenerator {
    private readonly processor_: SequentialProcessor<OperationRequest, number>;
    private readCurrentValue(): Promise<number> {
        const content = fs.readFileSync(this.filePath, "utf-8");
        const currentValue = parseInt(content, 10);
        return Promise.resolve(isNaN(currentValue) ? MIN_INT : currentValue);
    }
    private writeCurrentValue(currentValue: number): Promise<void> {
        fs.writeFileSync(this.filePath, currentValue.toString(), "utf-8");
        return Promise.resolve();
    }
    constructor(public filePath: string) {
        this.processor_ = new SequentialProcessor(async (input: OperationRequest) => {
            if (input.operation === "reset") {
                await this.writeCurrentValue(MIN_INT);
                return MIN_INT;
            }
            else if (input.operation === "set" && typeof input.value === "number") {
                const value = Math.max(MIN_INT, Math.min(MAX_INT, input.value));
                await this.writeCurrentValue(value);
                return value;
            }
            else if (input.operation === "next") {
                const currentValue = await this.readCurrentValue();
                const nextValue = (currentValue >= MAX_INT ? MIN_INT : currentValue + 1);
                await this.writeCurrentValue(nextValue);
                return nextValue;
            } else {
                throw new Error(`Invalid operation: ${input.operation}`);
            }
        });
    }
    async current(): Promise<number> {
        return this.readCurrentValue();
    }
    async next(): Promise<number> {
        return this.processor_.submit({ operation: "next" });
    }
    async reset(): Promise<number> {
        return this.processor_.submit({ operation: "reset" });
    }
    async set(value: number): Promise<number> {
        return this.processor_.submit({ operation: "set", value });
    }
    get idle(): boolean {
        return this.processor_.idle;
    }
    get pending(): number {
        return this.processor_.pending;
    }
}