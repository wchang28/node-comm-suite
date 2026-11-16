import sql from 'mssql';
import pg from 'pg';

export type SupportedDBType = "mssql" | "postgresql";

export type MSSQLConnectConfig = sql.config;
export type PostgresConnectConfig = pg.PoolConfig;

export interface DBConfig {
    type: SupportedDBType;
    connectConfig: MSSQLConnectConfig | PostgresConnectConfig;
    schema: string;
}

// connection settings for each supported database
export type DBPoolConfigs = {
    [K in SupportedDBType]?: K extends "mssql" ? MSSQLConnectConfig : PostgresConnectConfig;
};

// shape of ./db-configs.json
export interface DBConfigsFile {
    db_pool_configs: DBPoolConfigs;
    dbType: SupportedDBType;    // selects which entry of db_pool_configs to use
    dbSchema: string;
}

// picks the connection settings selected by dbType and returns them as a DBConfig
export function toDBConfig(file: DBConfigsFile): DBConfig {
    const {db_pool_configs, dbType, dbSchema} = file;
    if (dbType !== "mssql" && dbType !== "postgresql") {
        throw new Error(`Unsupported DB type: ${dbType}`);
    }
    const connectConfig = db_pool_configs[dbType];
    if (!connectConfig) {
        throw new Error(`db_pool_configs has no entry for dbType "${dbType}"`);
    }
    if (!dbSchema) {
        throw new Error(`dbSchema is missing`);
    }
    return {type: dbType, connectConfig, schema: dbSchema};
}
