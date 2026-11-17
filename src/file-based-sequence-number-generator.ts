import {SequentialProcessor} from "./sequential-processor.js";
import fs from "node:fs";

const MIN_INT = 1;
const MAX_INT = 2147483647; // max INT type value for SQL

export class FileBasedSequenceNumberGenerator {
    private readonly processor_: SequentialProcessor<void, number>;
    private readCurrentValue(filePath: string): Promise<number> {
        const content = fs.readFileSync(filePath, "utf-8");
        const currentValue = parseInt(content, 10);
        return Promise.resolve(isNaN(currentValue) ? MIN_INT : currentValue);
    }
    private writeCurrentValue(filePath: string, currentValue: number): Promise<void> {
        fs.writeFileSync(filePath, currentValue.toString(), "utf-8");
        return Promise.resolve();
    }
    constructor(public filePath: string) {
        this.processor_ = new SequentialProcessor(async () => {
            const currentValue = await this.readCurrentValue(this.filePath);
            const nextValue = (currentValue >= MAX_INT ? MIN_INT : currentValue + 1);
            await this.writeCurrentValue(this.filePath, nextValue);
            return nextValue;
        });
    }
    async current(): Promise<number> {
        return this.readCurrentValue(this.filePath);
    }
    async next(): Promise<number> {
        return this.processor_.submit();
    }
}