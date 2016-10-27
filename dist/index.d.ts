/// <reference types="nanomsg" />
/// <reference types="node" />
import { Socket } from 'nanomsg';
import { Pool, Client as PGClient } from 'pg';
import { RedisClient } from 'redis';
export interface CmdPacket {
    cmd: string;
    args: any[];
}
export declare type Permission = [string, boolean];
export interface ServerContext {
    domain: string;
    ip: string;
    uid: string;
    cache: RedisClient;
    publish: ((pkg: CmdPacket) => void);
}
export interface ServerFunction {
    (ctx: ServerContext, rep: ((result: any) => void), ...rest: any[]): void;
}
export declare class Server {
    serveraddr: string;
    queueaddr: string;
    rep: Socket;
    pub: Socket;
    functions: Map<string, ServerFunction>;
    permissions: Map<string, Map<string, boolean>>;
    constructor();
    init(serveraddr: string, queueaddr: string, cache: RedisClient): void;
    call(fun: string, permissions: Permission[], impl: ServerFunction): void;
}
export interface ProcessorFunction {
    (db: PGClient, cache: RedisClient, done: (() => void), ...args: any[]): void;
}
export declare class Processor {
    queueaddr: string;
    sock: Socket;
    functions: Map<string, ProcessorFunction>;
    constructor();
    init(queueaddr: string, pool: Pool, cache: RedisClient): void;
    call(cmd: string, impl: ProcessorFunction): void;
}
export interface Config {
    serveraddr: string;
    queueaddr: string;
    dbhost: string;
    dbuser: string;
    dbport?: number;
    database: string;
    dbpasswd: string;
    cachehost: string;
    cacheport?: number;
}
export declare class Service {
    config: Config;
    server: Server;
    processors: Processor[];
    constructor(config: Config);
    registerServer(server: Server): void;
    registerProcessor(processor: Processor): void;
    run(): void;
}
export declare function async_serial<T>(ps: Promise<T>[], scb: (vals: T[]) => void, fcb: (e: Error) => void): void;
export declare function async_serial_ignore<T>(ps: Promise<T>[], cb: (vals: T[]) => void): void;
export declare function fib(n: number): any;
export declare function wait_for_response(cache: RedisClient, reply: string, rep: ((result: any) => void)): void;
