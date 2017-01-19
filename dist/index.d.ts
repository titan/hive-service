/// <reference types="node" />
/// <reference types="nanomsg" />
import { Socket } from "nanomsg";
import { Pool, Client as PGClient } from "pg";
import { RedisClient } from "redis";
declare module "redis" {
    interface RedisClient extends NodeJS.EventEmitter {
        decrAsync(key: string): Promise<any>;
        delAsync(key: string): Promise<any>;
        hdelAsync(key: string, field: string): Promise<any>;
        hgetAsync(key: string, field: string): Promise<any>;
        hgetallAsync(key: string): Promise<any>;
        hincrbyAsync(key: string, field: string, value: number): Promise<any>;
        hkeysAsync(key: string): Promise<any>;
        hsetAsync(key: string, field: string, value: string | Buffer): Promise<any>;
        hvalsAsync(key: string): Promise<any>;
        incrAsync(key: string): Promise<any>;
        lindexAsync(key: string, index: number): Promise<any>;
        lpopAsync(key: string): Promise<any>;
        lpushAsync(key: string, value: string | number | Buffer): Promise<any>;
        lrangeAsync(key: string, start: number, stop: number): Promise<any>;
        lremAsync(key: string, count: number, value: string | number | Buffer): Promise<any>;
        rpopAsync(key: string): Promise<any>;
        rpoplpushAsync(source: string, destination: string): Promise<any>;
        rpushAsync(key: string, value: string | number | Buffer): Promise<any>;
        saddAsync(key: string, mumber: string | number | Buffer): Promise<any>;
        setAsync(key: string, value: string | number | Buffer): Promise<any>;
        setexAsync(key: string, ttl: number, value: string | number | Buffer): Promise<any>;
        sismemberAsync(key: string, value: string | Buffer): Promise<any>;
        zaddAsync(key: string, score: number, member: string | number | Buffer): Promise<any>;
        zcountAsync(key: string, min: string | number, max: string | number): Promise<any>;
        zrangeAsync(key: string, start: number, stop: number): Promise<any>;
        zrevrangebyscoreAsync(key: string, start: number | string, stop: number | string, limit?: string, offset?: number, count?: number): Promise<any>;
    }
    interface Multi extends NodeJS.EventEmitter {
        execAsync(): Promise<any>;
    }
}
export interface CmdPacket {
    sn?: string;
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
export interface AsyncServerFunction {
    (ctx: ServerContext, ...reset: any[]): Promise<any>;
}
export declare class Server {
    queueaddr: string;
    rep: Socket;
    pub: Socket;
    pair: Socket;
    functions: Map<string, [boolean, ServerFunction | AsyncServerFunction]>;
    permissions: Map<string, Map<string, boolean>>;
    constructor();
    init(serveraddr: string, queueaddr: string, cache: RedisClient): void;
    call(fun: string, permissions: Permission[], name: string, description: string, impl: ServerFunction): void;
    callAsync(fun: string, permissions: Permission[], name: string, description: string, impl: AsyncServerFunction): void;
}
export interface ProcessorContext {
    db: PGClient;
    cache: RedisClient;
    done: (() => void);
    publish: ((pkg: CmdPacket) => void);
}
export interface ProcessorFunction {
    (ctx: ProcessorContext, ...args: any[]): void;
}
export interface AsyncProcessorFunction {
    (ctx: ProcessorContext, ...args: any[]): Promise<any>;
}
export declare class Processor {
    queueaddr: string;
    sock: Socket;
    pub: Socket;
    functions: Map<string, [boolean, ProcessorFunction | AsyncProcessorFunction]>;
    subqueueaddr: string;
    subprocessors: Processor[];
    constructor(subqueueaddr?: string);
    init(queueaddr: string, pool: Pool, cache: RedisClient): void;
    registerSubProcessor(processor: Processor): void;
    call(cmd: string, impl: ProcessorFunction): void;
    callAsync(cmd: string, impl: AsyncProcessorFunction): void;
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
export declare function fib(n: number): number;
export declare function fiball(n: number): number;
export declare function wait_for_response(cache: RedisClient, reply: string, rep: ((result: any) => void), retry?: number): void;
export declare function set_for_response(cache: RedisClient, key: string, value: any, timeout?: number): Promise<any>;
export declare function rpc<T>(domain: string, addr: string, uid: string, fun: string, ...args: any[]): Promise<T>;
export interface Paging<T> {
    count: number;
    offset: number;
    limit: number;
    data: T[];
}
export declare function msgpack_encode(obj: any): Promise<Buffer>;
export declare function msgpack_decode<T>(buf: Buffer): Promise<T>;
