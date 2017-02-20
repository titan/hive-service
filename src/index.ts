import * as msgpack from "msgpack-lite";
import * as crypto from "crypto";
import { Socket, socket } from "nanomsg";
import * as fs from "fs";
import * as ip from "ip";
import * as bluebird from "bluebird";
import * as zlib from "zlib";
import { Pool, Client as PGClient } from "pg";
import { createClient, RedisClient, Multi } from "redis";
import { Disq } from "hive-disque";

declare module "redis" {
  export interface RedisClient extends NodeJS.EventEmitter {
    decrAsync(key: string): Promise<any>;
    decrbyAsync(key: string, decrement: number): Promise<any>;
    delAsync(key: string): Promise<any>;
    getAsync(key: string): Promise<any>;
    hdelAsync(key: string, field: string): Promise<any>;
    hgetAsync(key: string, field: string): Promise<any>;
    hgetallAsync(key: string): Promise<any>;
    hincrbyAsync(key: string, field: string, value: number): Promise<any>;
    hkeysAsync(key: string): Promise<any>;
    hsetAsync(key: string, field: string, value: string | Buffer): Promise<any>;
    hvalsAsync(key: string): Promise<any>;
    incrAsync(key: string): Promise<any>;
    incrbyAsync(key: string, increment: number): Promise<any>;
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
  export interface Multi extends NodeJS.EventEmitter {
    execAsync(): Promise<any>;
  }
}

export interface CmdPacket {
  sn?: string;
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
  push: (queuename: string, sn: string, data: any) => void;
  sn: string;
}

export interface ServerFunction {
  (ctx: ServerContext, rep: ((result: any) => void), ...rest: any[]): void;
}

export interface AsyncServerFunction {
  (ctx: ServerContext, ...reset: any[]): Promise<any>;
}

function zlib_deflate(payload: Buffer): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    zlib.deflate(payload, (e: Error, newbuf: Buffer) => {
      if (e) {
        reject(e);
      } else {
        resolve(newbuf);
      }
    });
  });
}

function server_msgpack(sn: string, obj: any, callback: ((buf: Buffer) => void)) {
  const payload = msgpack.encode(obj);
  if (payload.length > 1024) {
    zlib.deflate(payload, (e: Error, newbuf: Buffer) => {
      if (e) {
        callback(msgpack.encode({ sn, payload }));
      } else {
        callback(msgpack.encode({ sn, payload: newbuf }));
      }
    });
  } else {
    callback(msgpack.encode({ sn, payload }));
  }
}

export class Server {
  queueaddr: string;
  rep: Socket;
  pub: Socket;
  pair: Socket;
  queue: Disq;

  functions: Map<string, [boolean, ServerFunction | AsyncServerFunction]>;
  permissions: Map<string, Map<string, boolean>>; // {function => { domain => permission }}

  constructor() {
    this.functions = new Map<string, [boolean, ServerFunction | AsyncServerFunction]>();
    this.permissions = new Map<string, Map<string, boolean>>();
  }

  public init(serveraddr: string, queueaddr: string, cache: RedisClient, queue?: Disq): void {
    this.queueaddr = queueaddr;
    this.rep = socket("rep");
    this.rep.bind(serveraddr);
    this.pub = socket("pub");
    this.pub.bind(this.queueaddr);
    const lastnumber = parseInt(serveraddr[serveraddr.length - 1]) + 1;
    const newaddr = serveraddr.substr(0, serveraddr.length - 1) + lastnumber.toString();
    this.pair = socket("pair");
    this.pair.bind(newaddr);
    this.queue = queue;

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
          publish: undefined,
          push: undefined,
          sn,
        };
        for (const key in pkt.ctx /* Domain, IP, User */) {
          ctx[key] = pkt.ctx[key];
        }
        ctx.cache = cache;
        const fun: string = pkt.fun;
        const args: any[] = pkt.args;
        if (_self.permissions.has(fun) && _self.permissions.get(fun).get(ctx.domain)) {
          const [asynced, impl] = _self.functions.get(fun);
          ctx.publish = (pkt: CmdPacket) => _self.pub.send(msgpack.encode({...pkt, sn}));
          ctx.push = (queuename: string, sn: string, data: any) => {
            const event = {
              sn,
              data
            };
            if (_self.queue) {
              msgpack_encode(event).then(pkt => {
                _self.queue.addjob(queuename, pkt).then();
              });
            }
          }
          if (!asynced) {
            const func = impl as ServerFunction;
            args ?
            func(ctx, (result: any) => {
              server_msgpack(sn, result, (buf: Buffer) => { sock.send(buf); });
            }, ...args) :
            func(ctx, (result: any) => {
              server_msgpack(sn, result, (buf: Buffer) => { sock.send(buf); });
            });
          } else {
            const func = impl as AsyncServerFunction;
            func(ctx, ...args).then(result => {
              server_msgpack(sn, result, (buf: Buffer) => { sock.send(buf); });
            }).catch(e => {
              const payload = msgpack.encode({ code: 500, msg: e.message });
              msgpack_encode({ sn, payload }).then(pkt => {
                sock.send(pkt);
              });
            });
          }
        } else {
          const payload = msgpack.encode({ code: 403, msg: "Forbidden" });
          sock.send(msgpack.encode({ sn, payload }));
        }
      });
    }
  }

  public call(fun: string, permissions: Permission[], name: string, description: string, impl: ServerFunction): void {
    this.functions.set(fun,[false, impl]);
    this.permissions.set(fun, new Map(permissions));
  }

  public callAsync(fun: string, permissions: Permission[], name: string, description: string, impl: AsyncServerFunction): void {
    this.functions.set(fun, [true, impl]);
    this.permissions.set(fun, new Map(permissions));
  }
}

export interface ProcessorContext {
  db: PGClient;
  cache: RedisClient;
  done: ((result?: any) => void);
  publish: ((pkg: CmdPacket) => void);
}

export interface ProcessorFunction {
  (ctx: ProcessorContext, ...args: any[]): void;
}

export interface AsyncProcessorFunction {
  (ctx: ProcessorContext, ...args: any[]): Promise<any>;
}

export class Processor {
  queueaddr: string;
  sock: Socket;
  pub: Socket;
  functions: Map<string, [boolean, ProcessorFunction | AsyncProcessorFunction]>;
  subqueueaddr: string;
  subprocessors: Processor[];

  constructor(subqueueaddr?: string) {
    this.functions = new Map<string, [boolean, ProcessorFunction | AsyncProcessorFunction]>();
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
        const [asynced, impl] = _self.functions.get(pkt.cmd);
        pool.connect().then(db => {
          const ctx: ProcessorContext = {
            db,
            cache,
            done: !asynced ? (result?: any) => {
              if (result !== undefined) {
                msgpack_encode(result).then(buf => {
                  cache.setex(`results:${pkt.sn}`, 600, buf, (e: Error, _: any) => {
                    if (e) {
                      console.log("Error " + e.stack);
                    }
                  });
                }).catch(e => {
                  console.log("Error " + e.stack);
                });
              }
            } : undefined,
            publish: (pkt: CmdPacket) => _self.pub ? _self.pub.send(msgpack.encode(pkt)) : undefined
          };
          if (!asynced) {
            const func = impl as ProcessorFunction;
            try {
              func(ctx, ...pkt.args);
            } catch (e) {
              console.log("Error " + e.stack);
            } finally {
              db.release();
            }
          } else {
            const func = impl as AsyncProcessorFunction;
            func(ctx, ...pkt.args).then(result => {
              db.release();
              if (result !== undefined) {
                msgpack_encode(result).then(buf => {
                  cache.setex(`results:${pkt.sn}`, 600, buf, (e: Error, _: any) => {
                    if (e) {
                      console.log("Error " + e.stack);
                    }
                  });
                }).catch(e => {
                  console.log("Error " + e.stack);
                });
              }
            }).catch(e => {
              db.release();
              console.log("Error " + e.stack);
              msgpack_encode({ code: 500, msg: e.message }).then(buf => {
                cache.setex(`results:${pkt.sn}`, 600, buf, (e: Error, _: any) => {
                  if (e) {
                    console.log("Error " + e.stack);
                  }
                });
              }).catch(e => {
                console.log("Error " + e.stack);
              });
            });
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
    this.functions.set(cmd, [false, impl]);
  }

  public callAsync(cmd: string, impl: AsyncProcessorFunction): void {
    this.functions.set(cmd, [true, impl]);
  }
}

export interface BusinessEventPacket {
  sn: string;
  data: any;
}

export interface BusinessEventContext {
  pool: Pool;
  cache: RedisClient;
  queue: Disq;
  queuename: string;
  handler: BusinessEventHandlerFunction;
  db?: PGClient;
}

export interface BusinessEventHandlerFunction {
  (ctx: BusinessEventContext, data: any): Promise<any>;
}

function on_event_timer(thiz:BusinessEventListener, ctx: BusinessEventContext) {
  const options = {
    timeout: 10,
    count: 1,
  };
  ctx.queue.getjob(ctx.queuename, options).then(job => {
    if (job) {
      const body = job.body as Buffer;
      msgpack_decode(body).then((pkt: BusinessEventPacket) => {
        ctx.pool.connect().then(db => {
          ctx.db = db;
          ctx.handler(ctx, pkt.data).then(result => {
            db.release();
            if (result !== undefined) {
              msgpack_encode(result).then(buf => {
                ctx.cache.setex(`results:${pkt.sn}`, 600, buf, (e: Error, _: any) => {
                  if (e) {
                    console.log("Error " + e.stack);
                  }
                  ctx.queue.ackjob(job.id);
                  setTimeout(on_event_timer, 0, ctx);
                });
              }).catch(e => {
                console.log("Error " + e.stack);
                ctx.queue.ackjob(job.id);
                setTimeout(on_event_timer, 0, ctx);
              });
            }
          }).catch(e => {
            db.release();
            msgpack_encode({ code: 500, msg: e.message }).then(buf => {
              ctx.cache.setex(`results:${pkt.sn}`, 600, buf, (e: Error, _: any) => {
                if (e) {
                  console.log("Error " + e.stack);
                }
                setTimeout(on_event_timer, 1000, ctx);
              });
            }).catch(e => {
              console.log("Error " + e.stack);
              setTimeout(on_event_timer, 1000, ctx);
            });
          });
        }).catch(e => {
          console.log("DB connection error " + e.stack);
          setTimeout(on_event_timer, 1000, ctx);
        });
      }).catch(e => {
        console.log("Invalid event packet" + e.stack);
        ctx.queue.ackjob(job.id);
        setTimeout(on_event_timer, 1000, ctx);
      });
    } else {
      setTimeout(on_event_timer, 1000, ctx);
    }
  }).catch(e => {
    setTimeout(on_event_timer, 1000, ctx);
  });
}

export class BusinessEventListener {
  queuename: string;
  handler: BusinessEventHandlerFunction;
  constructor(queuename: string) {
    this.queuename = queuename;
  }

  public init(pool: Pool, cache: RedisClient, queue: Disq): void {
    const ctx: BusinessEventContext = {
      pool,
      cache,
      queue,
      queuename: this.queuename,
      handler: this.handler,
      db: undefined
    };
    setTimeout(on_event_timer, 1000, ctx);
  }

  public onEvent(handler: BusinessEventHandlerFunction) {
    this.handler = handler;
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
  queuehost?: string;
  queueport?: number;
}

export class Service {
  config: Config;
  server: Server;
  processors: Processor[];
  listeners: BusinessEventListener[];

  constructor(config: Config) {
    this.config = config;
    this.processors = [];
    this.listeners = [];
  }

  public registerServer(server: Server): void {
    this.server = server;
  }

  public registerProcessor(processor: Processor): void {
    this.processors.push(processor);
  }

  public registerEventListener(listener: BusinessEventListener): void {
    this.listeners.push(listener);
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

    for (const processor of this.processors) {
      processor.init(this.config.queueaddr, pool, cacheAsync);
    }

    if (this.config.queuehost) {
      const port = this.config.queueport ? this.config.queueport : 7711;
      const queue = new Disq({nodes: [`${this.config.queuehost}:${port}`]});
      this.server.init(this.config.serveraddr, this.config.queueaddr, cacheAsync, queue);
      for (const listener of this.listeners) {
        listener.init(pool, cacheAsync, queue);
      }
    } else {
      this.server.init(this.config.serveraddr, this.config.queueaddr, cacheAsync);
    }
  }
}

function fib_iter(a: number, b: number, p: number, q: number, n: number): number {
  if (n === 0) {
    return b;
  }
  if (n % 2 === 0) {
    return fib_iter(a, b, p * p + q * q, 2 * p * q + q * q, n / 2);
  }
  return fib_iter(a * p + a * q + b * q, b * p + a * q, p, q, n - 1);
}

const fibs = [0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, 6765, 10946, 17711, 28657, 46368, 75025, 121393, 196418, 317811, 514229, 832040, 1346269, 2178309, 3524578, 5702887, 9227465, 14930352, 24157817, 39088169, 63245986, 102334155, 165580141, 267914296, 433494437, 701408733, 1134903170, 1836311903, 2971215073, 4807526976, 7778742049, 12586269025, 20365011074, 32951280099, 53316291173, 86267571272, 139583862445, 225851433717, 365435296162, 591286729879, 956722026041, 1548008755920, 2504730781961, 4052739537881, 6557470319842, 10610209857723, 17167680177565, 27777890035288, 44945570212853, 72723460248141, 117669030460994, 190392490709135, 308061521170129, 498454011879264, 806515533049393, 1304969544928657, 2111485077978050, 3416454622906707, 5527939700884757, 8944394323791464, 14472334024676220, 23416728348467690, 37889062373143900, 61305790721611590, 99194853094755490, 160500643816367070, 259695496911122600, 420196140727489660, 679891637638612200, 1100087778366102000, 1779979416004714200, 2880067194370816000, 4660046610375530000, 7540113804746346000, 12200160415121877000, 19740274219868226000, 31940434634990100000, 51680708854858320000, 83621143489848430000, 135301852344706740000, 218922995834555140000]

export function fib(n: number): number {
  return fibs[n];
}

export function fiball(n: number): number {
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

export function waiting(ctx: ServerContext, rep: ((result: any) => void), retry: number = 7) {
  setTimeout(timer_callback, 100, ctx.cache, `results:${ctx.sn}`, rep, retry + 1, retry);
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

function async_timer_callback(cache: RedisClient, reply: string, resolve, reject, retry: number, countdown: number) {
  cache.get(reply, (err: Error, result: Buffer) => {
    if (result) {
      msgpack_decode<any>(result).then(obj => {
        resolve(obj);
      }).catch((e: Error) => {
        reject(e);
      });
    } else if (countdown === 0) {
      const e = new Error();
      e.name = "504";
      e.message = "Request Timeout";
      reject(e);
    } else {
      setTimeout(timer_callback, fib(retry - countdown) * 1000, cache, reply, resolve, reject, retry, countdown - 1);
    }
  });
}

export function waitingAsync(ctx: ServerContext, retry: number = 7): Promise<any> {
  return new Promise<any>((resolve, reject) => {
    setTimeout(async_timer_callback, 100, ctx.cache, `results:${ctx.sn}`, resolve, reject, retry + 1, retry);
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
