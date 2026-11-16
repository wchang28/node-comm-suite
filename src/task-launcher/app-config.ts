import {SystemLoadingMonitorConfig} from "../type.js";
import {DBConfig} from "../db-types.js";

export interface CPUConfig {
    reservedCPUs?: number | null;
    availableCPUs?: number | null;
}

export interface AppConfig {
    dispatcherSSEUrl: string;
    cpuConfig?: CPUConfig;
    dbConfig: DBConfig;
    listenHost?: string | null;
    listenPort?: number | null;
    systemLoadingMonitorConfig: SystemLoadingMonitorConfig;
    mainDiskDrivePath?: string | null;
}