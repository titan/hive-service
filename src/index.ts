import * as msgpack from "msgpack-lite";
import * as crypto from "crypto";
import { Socket, socket } from "nanomsg";
import * as fs from "fs";
import * as ip from "ip";
import * as bluebird from "bluebird";
import * as zlib from "zlib";
import { Pool, Client as PGClient } from "pg";
import { createClient, RedisClient, Multi } from "redis";

declare module "redis" {
  export interface RedisClient extends NodeJS.EventEmitter {
    incrAsync(key: string): Promise<any>;
    hgetAsync(key: string, field: string): Promise<any>;
    hincrbyAsync(key: string, field: string, value: number): Promise<any>;
    lpushAsync(key: string, value: string | number): Promise<any>;
    setexAsync(key: string, ttl: number, value: string | Buffer): Promise<any>;
    zrevrangebyscoreAsync(key: string, start: number, stop: number): Promise<any>;
  }
  export interface Multi extends NodeJS.EventEmitter {
    execAsync(): Promise<any>;
  }
}

export interface CmdPacket {
  cmd: string;
  args: any[];
}

export type Permission = [string, boolean];

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

export class Server {
  queueaddr: string;
  rep: Socket;
  pub: Socket;
  pair: Socket;

  functions: Map<string, ServerFunction>;
  permissions: Map<string, Map<string, boolean>>; // {function => { domain => permission }}

  constructor() {
    this.functions = new Map<string, ServerFunction>();
    this.permissions = new Map<string, Map<string, boolean>>();
  }

  public init(serveraddr: string, queueaddr: string, cache: RedisClient): void {
    this.queueaddr = queueaddr;
    this.rep = socket("rep");
    this.rep.bind(serveraddr);
    this.pub = socket("pub");
    this.pub.bind(this.queueaddr);
    const lastnumber = parseInt(serveraddr[serveraddr.length - 1]) + 1;
    const newaddr = serveraddr.substr(0, serveraddr.length - 1) + lastnumber.toString();
    this.pair = socket("pair");
    this.pair.bind(newaddr);

    const _self = this;

    for (const sock of [this.pair, this.rep]) {
      sock.on("data", function (buf: NodeBuffer) {
        const data = msgpack.decode(buf);
        const pkt = data.pkt;
        const sn = data.sn;
        const ctx: ServerContext = {
          domain: undefined,
          ip: undefined,
          uid: undefined,
          cache: undefined,
          publish: undefined
        };
        for (const key in pkt.ctx /* Domain, IP, User */) {
          ctx[key] = pkt.ctx[key];
        }
        ctx.cache = cache;
        const fun: string = pkt.fun;
        const args: any[] = pkt.args;
        if (_self.permissions.has(fun) && _self.permissions.get(fun).get(ctx.domain)) {
          const func: ServerFunction = _self.functions.get(fun);
          if (args != null) {
            ctx.publish = (pkt: CmdPacket) => _self.pub.send(msgpack.encode(pkt));
            func(ctx, function(result) {
              const payload = msgpack.encode(result);
              if (payload.length > 1024) {
                zlib.deflate(payload, (e: Error, newbuf: Buffer) => {
                  if (e) {
                    sock.send(msgpack.encode({ sn, payload }));
                  } else {
                    sock.send(msgpack.encode({ sn, payload: newbuf }));
                  }
                });
              } else {
                sock.send(msgpack.encode({ sn, payload }));
              }
            }, ...args);
          } else {
            ctx.publish = (pkt: CmdPacket) => _self.pub.send(msgpack.encode(pkt));
            func(ctx, function(result) {
              const payload = msgpack.encode(result);
              if (payload.length > 1024) {
                zlib.deflate(payload, (e: Error, newbuf: Buffer) => {
                  if (e) {
                    sock.send(msgpack.encode({ sn, payload }));
                  } else {
                    sock.send(msgpack.encode({ sn, payload: newbuf }));
                  }
                });
              } else {
                sock.send(msgpack.encode({ sn, payload }));
              }
            });
          }
        } else {
          const payload = msgpack.encode({code: 403, msg: "Forbidden"});
          sock.send(msgpack.encode({ sn, payload }));
        }
      });
    }
  }

  public call(fun: string, permissions: Permission[], name: string, description: string, impl: ServerFunction): void {
    this.functions.set(fun, impl);
    this.permissions.set(fun, new Map(permissions));
  }
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

export class Processor {
  queueaddr: string;
  sock: Socket;
  pub: Socket;
  functions: Map<string, ProcessorFunction>;
  subqueueaddr: string;
  subprocessors: Processor[];

  constructor(subqueueaddr?: string) {
    this.functions = new Map<string, ProcessorFunction>();
    this.subprocessors = [];
    if (subqueueaddr) {
      this.subqueueaddr = subqueueaddr;
      const path = subqueueaddr.substring(subqueueaddr.indexOf("///") + 2, subqueueaddr.length);
      if (fs.existsSync(path)) {
        fs.unlinkSync(path); // make nanomsg happy
      }
    }
  }

  public init(queueaddr: string, pool: Pool, cache: RedisClient): void {
    this.queueaddr = queueaddr;
    this.sock = socket("sub");
    this.sock.connect(this.queueaddr);
    if (this.subqueueaddr) {
      this.pub = socket("pub");
      this.pub.bind(this.subqueueaddr);
      for (const subprocessor of this.subprocessors) {
        subprocessor.init(this.subqueueaddr, pool, cache);
      }
    }
    const _self = this;
    this.sock.on("data", (buf: NodeBuffer) => {
      const pkt: CmdPacket = msgpack.decode(buf);
      if (_self.functions.has(pkt.cmd)) {
        pool.connect().then(db => {
          const func = _self.functions.get(pkt.cmd);
          let ctx: ProcessorContext = {
            db,
            cache,
            done: () => { db.release(); },
            publish: (pkt: CmdPacket) => _self.pub ? _self.pub.send(msgpack.encode(pkt)) : undefined,
          };
          if (pkt.args) {
            func(ctx, ...pkt.args);
          } else {
            func(ctx);
          }
        }).catch(e => {
          console.log("DB connection error " + e.stack);
        });
      } else {
        console.error(pkt.cmd + " not found!");
      }
    });
  }

  public registerSubProcessor(processor: Processor): void {
    this.subprocessors.push(processor);
  }

  public call(cmd: string, impl: ProcessorFunction): void {
    this.functions.set(cmd, impl);
  }
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

export class Service {
  config: Config;
  server: Server;
  processors: Processor[];

  constructor(config: Config) {
    this.config = config;
    this.processors = [];
  }

  public registerServer(server: Server): void {
    this.server = server;
  }

  public registerProcessor(processor: Processor): void {
    this.processors.push(processor);
  }

  public run(): void {
    const path = this.config.queueaddr.substring(this.config.queueaddr.indexOf("///") + 2, this.config.queueaddr.length);
    if (fs.existsSync(path)) {
      fs.unlinkSync(path); // make nanomsg happy
    }

    const cache: RedisClient = createClient(this.config.cacheport ? this.config.cacheport : 6379, this.config.cachehost, { "return_buffers": true });
    const cacheAsync : RedisClient = bluebird.promisifyAll(cache) as RedisClient;
    const dbconfig = {
      host: this.config.dbhost,
      user: this.config.dbuser,
      database: this.config.database,
      password: this.config.dbpasswd,
      port: this.config.dbport ? this.config.dbport : 5432,
      min: 1, // min number of clients in the pool
      max: 2 * this.processors.length, // max number of clients in the pool
      idleTimeoutMillis: 30000, // how long a client is allowed to remain idle before being closed
    };
    const pool = new Pool(dbconfig);

    this.server.init(this.config.serveraddr, this.config.queueaddr, cacheAsync);
    for (const processor of this.processors) {
      processor.init(this.config.queueaddr, pool, cacheAsync);
    }
  }
}

export function async_serial<T>(ps: Promise<T>[], scb: (vals: T[]) => void, fcb: (e: Error) => void) {
  _async_serial<T>(ps, [], scb, fcb);
}

function _async_serial<T>(ps: Promise<T>[], acc: T[], scb: (vals: T[]) => void, fcb: (e: Error) => void) {
  if (ps.length === 0) {
    scb(acc);
  } else {
    let p = ps.shift();
    p.then(val => {
      acc.push(val);
      _async_serial(ps, acc, scb, fcb);
    }).catch((e: Error) => {
      fcb(e);
    });
  }
}

export function async_serial_ignore<T>(ps: Promise<T>[], cb: (vals: T[]) => void) {
  _async_serial_ignore<T>(ps, [], cb);
}

function _async_serial_ignore<T>(ps: Promise<T>[], acc: T[], cb: (vals: T[]) => void) {
  if (ps.length === 0) {
    cb(acc);
  } else {
    let p = ps.shift();
    p.then(val => {
      acc.push(val);
      _async_serial_ignore(ps, acc, cb);
    }).catch((e: Error) => {
      _async_serial_ignore(ps, acc, cb);
    });
  }
}

function fib_iter(a: number, b: number, p: number, q: number, n: number) {
  if (n === 0) {
    return b;
  }
  if (n % 2 === 0) {
    return fib_iter(a, b, p * p + q * q, 2 * p * q + q * q, n / 2);
  }
  return fib_iter(a * p + a * q + b * q, b * p + a * q, p, q, n - 1);
}

export function fib(n: number) {
  return fib_iter(1, 0, 0, 1, n);
}

function timer_callback(cache: RedisClient, reply: string, rep: ((result: any) => void), retry: number, countdown: number) {
  cache.get(reply, (err: Error, result: Buffer) => {
    if (result) {
      msgpack_decode<any>(result).then(obj => {
        rep(obj);
      }).catch((e: Error) => {
        rep({
          code: 540,
          msg: e.message
        });
      });
    } else if (countdown === 0) {
      rep({
        code: 504,
        msg: "Request Timeout"
      });
    } else {
      setTimeout(timer_callback, fib(retry - countdown) * 1000, cache, reply, rep, retry, countdown - 1);
    }
  });
}

export function wait_for_response(cache: RedisClient, reply: string, rep: ((result: any) => void), retry: number = 7) {
  setTimeout(timer_callback, 500, cache, reply, rep, retry + 1, retry);
}

export function set_for_response(cache: RedisClient, key: string, value: any, timeout: number = 30): Promise<any> {
  return new Promise((resolve, reject) => {
    msgpack_encode(value).then(buf => {
      cache.setex(key, timeout, buf, (e: Error, _: any) => {
        if (e) {
          reject(e);
        } else {
          resolve();
        }
      });
    }).catch(e => {
      reject(e);
    });
  });
}

export function rpc<T>(domain: string, addr: string, uid: string, fun: string, ...args: any[]): Promise<T> {
  const p = new Promise<T>(function (resolve, reject) {
    let a = [];
    if (args != null) {
      a = [...args];
    }
    const params = {
      ctx: {
        domain: domain,
        ip:     ip.address(),
        uid:    uid
      },
      fun: fun,
      args: a
    };
    const sn = crypto.randomBytes(64).toString("base64");
    const req = socket("req");
    req.connect(addr);

    req.on("data", (msg) => {
      const data: Object = msgpack.decode(msg);
      if (sn === data["sn"]) {
        if (data["payload"][0] === 0x78 && data["payload"][1] === 0x9c) {
          zlib.inflate(data["payload"], (e: Error, newbuf: Buffer) => {
            if (e) {
              req.close();
              reject(e);
            } else {
              req.close();
              resolve(msgpack.decode(newbuf));
            }
          });
        } else {
          req.close();
          resolve(msgpack.decode(data["payload"]));
        }
      } else {
        req.close();
        reject(new Error("Invalid calling sequence number"));
      }
    });
    req.send(msgpack.encode({ sn, pkt: params }));
  });
  return p;
}

export interface Paging<T> {
  count: number;
  offset: number;
  limit: number;
  data: T[];
}

export function msgpack_encode(obj: any): Promise<Buffer> {
  return new Promise<Buffer>((resolve, reject) => {
    const buf = msgpack.encode(obj);
    if (buf.length > 1024) {
      return zlib.deflate(buf, (e: Error, newbuf: Buffer) => {
        if (e) {
          reject(e);
        } else {
          resolve(newbuf);
        }
      });
    } else {
      resolve(buf);
    }
  });
}

export function msgpack_decode<T>(buf: Buffer): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    if (buf[0] === 0x78 && buf[1] === 0x9c) {
      zlib.inflate(buf, (e: Error, newbuf: Buffer) => {
        if (e) {
          reject(e);
        } else {
          const result: T = msgpack.decode(newbuf) as T;
          resolve(result);
        }
      });
    } else {
      const result: T = msgpack.decode(buf) as T;
      resolve(result);
    }
  });
}
