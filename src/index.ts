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
import * as http from "http";

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
    keysAsync(key: string): Promise<any>;
    lindexAsync(key: string, index: number): Promise<any>;
    llenAsync(key: string): Promise<number>;
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
    smembersAsync(key: string): Promise<any>;
    zaddAsync(key: string, score: number, member: string | number | Buffer): Promise<any>;
    zcountAsync(key: string, min: string | number, max: string | number): Promise<number>;
    zrangeAsync(key: string, start: number, stop: number): Promise<any[]>;
    zrangebyscoreAsync(key: string, min: number | string, max: number | string, limit?: string, offset?: number, count?: number): Promise<any[]>;
    zrankAsync(key: string, member: string | number | Buffer): Promise<number>;
    zremAsync(key: string, value: any): Promise<any>;
    zremrangebyscoreAsync(key: string, start: number | string, stop: number | string): Promise<any>;
    zrevrangeAsync(key: string, start: number, stop: number): Promise<any[]>;
    zrevrangebyscoreAsync(key: string, start: number | string, stop: number | string, limit?: string, offset?: number, count?: number): Promise<any[]>;
  }
  export interface Multi extends NodeJS.EventEmitter {
    execAsync(): Promise<any>;
  }
}

export interface Context {
  modname: string;
  domain: string;
  uid: string;
  sn: string;
  cache: RedisClient;
  report: (level: number, error: Error) => void;
}

export interface ErrorPacket {
  module: string;
  function: string;
  level: number;
  error: Error;
  args?: any[];
};

export interface Result<T> {
  code: number;
  data?: T;
  msg?: string;
  now?: Date;
}

export interface CmdPacket {
  domain?: string;
  uid?: string;
  sn?: string;
  cmd: string;
  args: any[];
}

export type Permission = [string, boolean];

export class QueueProvider {
  addresses: string[];
  disque: Disq;
  constructor(addresses: string[]) {
    this.addresses = addresses;
  }

  public instance(): Disq {
    if (this.disque === undefined) {
      this.disque = new Disq({ nodes: this.addresses});
    }
    return this.disque;
  }

  public error(e: Error): void {
    if (e.message.indexOf("ECONNREFUSED") !== -1) {
      this.disque = undefined;
    }
  }
}

export interface ServerContext extends Context {
  ip: string;
  publish: ((pkg: CmdPacket) => void);
  push: (queuename: string, data: any, qsn?: string) => void;
}

export interface ServerFunction {
  (ctx: ServerContext, rep: ((result: Result<any>) => void), ...rest: any[]): void;
}

export interface AsyncServerFunction {
  (ctx: ServerContext, ...rest: any[]): Promise<Result<any>>;
}

function get_response_addresses(addr: string) {
  const lastnumber = parseInt(addr[addr.length - 1]);
  const prefix = addr.substr(0, addr.length - 1);
  const addresses = [];
  const limit = parseInt(process.env["MAX_GATEWAY"] || 3);
  for (let i = 0; i < limit; i ++) {
    addresses.push(prefix + (lastnumber + i).toString());
  }
  return addresses;
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
  pub: Socket;
  loginfo: Function;
  logerror: Function;
  queue_provider: QueueProvider;

  functions: Map<string, [boolean, ServerFunction | AsyncServerFunction]>;
  permissions: Map<string, Map<string, boolean>>; // {function => { domain => permission }}

  constructor() {
    this.functions = new Map<string, [boolean, ServerFunction | AsyncServerFunction]>();
    this.permissions = new Map<string, Map<string, boolean>>();
  }

  public init(modname: string, serveraddr: string, queueaddr: string, cache: RedisClient, loginfo: Function, logerror: Function, queue_provider?: QueueProvider): void {
    this.queueaddr = queueaddr;
    if (this.queueaddr) {
      this.pub = socket("pub");
      this.pub.bind(this.queueaddr);
    }
    this.loginfo = loginfo;
    this.logerror = logerror;
    this.queue_provider = queue_provider;
    for (const sock of get_response_addresses(serveraddr).map(x => { loginfo(x); const pair = socket("pair"); pair.bind(x); return pair; })) {
      sock.on("data", ((buf: NodeBuffer) => {
        const data = msgpack.decode(buf);
        const pkt = data.pkt;
        const sn = data.sn;
        const ctx: ServerContext = {
          ... pkt.ctx,
          modname,
          cache,
          publish: undefined,
          push: undefined,
          report: undefined,
          sn,
        };
        const fun: string = pkt.fun;
        const args: any[] = pkt.args;
        if (this.permissions.has(fun) && this.permissions.get(fun).get(ctx.domain)) {
          const [asynced, impl] = this.functions.get(fun);
          ctx.publish = this.pub ? (pkt: CmdPacket) => this.pub.send(msgpack.encode({...pkt, sn, domain: ctx.domain, uid: ctx.uid})) : (pkt: CmdPacket) => { logerror("Publish channel not exists"); };
          ctx.push = (queuename: string, data: any, qsn?: string) => {
            const event = {
              sn: qsn || sn,
              data,
              domain: ctx.domain,
              uid: ctx.uid,
            };
            if (this.queue_provider) {
              msgpack_encode(event, (e: Error, pkt: Buffer) => {
                if (e) {
                  logerror(e);
                } else {
                  this.queue_provider.instance().addjob(queuename, pkt, { retry: 0 }, () => {
                  }, (e: Error) => {
                    logerror(e);
                    this.queue_provider.error(e);
                  });
                }
              });
            }
          };
          ctx.report = this.queue_provider ? (level: number, error: Error) => {
            const payload: ErrorPacket = {
              module: modname,
              function: fun,
              level,
              error,
              args,
            };
            const pkt = msgpack.encode(payload);
            this.queue_provider.instance().addjob("hive-errors", pkt, { retry: 0 }, () => {}, (e: Error) => {
              this.logerror(e);
              this.queue_provider.error(e);
            });
          } : (level: number, error: Error) => {
          };
          if (!asynced) {
            const func = impl as ServerFunction;
            try {
              args ? func(ctx, (result: any) => {
                server_msgpack(sn, result, (buf: Buffer) => { sock.send(buf); });
              }, ...args) : func(ctx, (result: any) => {
                server_msgpack(sn, result, (buf: Buffer) => { sock.send(buf); });
              });
            } catch (e) {
              logerror(e);
              const payload = msgpack.encode({ code: 500, msg: e.message });
              msgpack_encode({ sn, payload }, (e: Error, pkt: Buffer) => {
                if (e) {
                  logerror(e);
                } else {
                  sock.send(pkt);
                }
              });
            }
          } else {
            const func = impl as AsyncServerFunction;
            const result = args ? func(ctx, ...args) : func(ctx);
            result.then(result => {
              server_msgpack(sn, result, (buf: Buffer) => { sock.send(buf); });
            }).catch(e => {
              logerror(e);
              ctx.report(0, e);
              const payload = msgpack.encode({ code: 500, msg: e.stack + " func: " + fun});
              msgpack_encode({ sn, payload }, (e: Error, pkt: Buffer) => {
                if (e) {
                  logerror(e);
                } else {
                  sock.send(pkt);
                }
              });
            });
          }
        } else {
          const payload = msgpack.encode({ code: 403, msg: "Forbidden" });
          sock.send(msgpack.encode({ sn, payload }));
        }
      }).bind(this));
    }
  }

  public call(fun: string, permissions: Permission[], name: string, description: string, impl: ServerFunction): void {
    this.functions.set(fun, [false, impl]);
    this.permissions.set(fun, new Map(permissions));
  }

  public callAsync(fun: string, permissions: Permission[], name: string, description: string, impl: AsyncServerFunction): void {
    this.functions.set(fun, [true, impl]);
    this.permissions.set(fun, new Map(permissions));
  }
}

function report_processor_error(ctx: ProcessorContext, fun: string, level: number, e: Error, args?: any[]) {
  ctx.logerror(e);
  const payload: ErrorPacket = {
    module: ctx.modname,
    function: fun,
    level: 0,
    error: e,
    args,
  };
  const pkt = msgpack.encode(payload);
  if (ctx.queue_provider) {
    ctx.queue_provider.instance().addjob("hive-errors", pkt, { retry: 0 }, () => {
    }, (e: Error) => {
      ctx.logerror(e);
      ctx.queue_provider.error(e);
    });
  }
}

export interface ProcessorContext extends Context {
  db: PGClient;
  queue_provider?: QueueProvider;
  publish: ((pkg: CmdPacket) => void);
  push: (queuename: string, data: any, qsn?: string) => void;
  logerror: Function;
}

export interface ProcessorFunction {
  (ctx: ProcessorContext, ...args: any[]): void;
}

export interface AsyncProcessorFunction {
  (ctx: ProcessorContext, ...args: any[]): Promise<Result<any>>;
}

export class Processor {
  modname: string;
  queueaddr: string;
  sock: Socket;
  pub: Socket;
  functions: Map<string, [boolean, ProcessorFunction | AsyncProcessorFunction]>;
  subqueueaddr: string;
  subprocessors: Processor[];
  queue_provider: QueueProvider;

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

  public init(modname: string, queueaddr: string, pool: Pool, cache: RedisClient, loginfo: Function, logerror: Function, queue_provider?: QueueProvider): void {
    this.modname = modname;
    this.queueaddr = queueaddr;
    this.sock = socket("sub");
    this.sock.connect(this.queueaddr);
    this.queue_provider = queue_provider;
    if (this.subqueueaddr) {
      this.pub = socket("pub");
      this.pub.bind(this.subqueueaddr);
      for (const subprocessor of this.subprocessors) {
        subprocessor.init(modname, this.subqueueaddr, pool, cache, loginfo, logerror, queue_provider);
      }
    }
    this.sock.on("data", ((buf: NodeBuffer) => {
      const pkt: CmdPacket = msgpack.decode(buf);
      if (this.functions.has(pkt.cmd)) {
        const [asynced, impl] = this.functions.get(pkt.cmd);
        pool.connect((err: Error, db: PGClient, done: () => void) => {
          if (err) {
            const ctx:  ProcessorContext = {
              modname,
              queue_provider,
              db: null,
              cache: null,
              domain: null,
              uid: null,
              sn: null,
              publish: null,
              push: null,
              report: null,
              logerror: null,
            };
            report_processor_error(ctx, "pool.connect", 0, err);
          } else {
            const ctx: ProcessorContext = {
              modname,
              db,
              cache,
              domain: pkt.domain,
              uid: pkt.uid,
              sn: pkt.sn,
              queue_provider,
              publish: (pkt: CmdPacket) => this.pub ? this.pub.send(msgpack.encode(pkt)) : undefined,
              push: (queuename: string, data: any, qsn?: string) => {
                const event = {
                  sn: qsn || pkt.sn,
                  data,
                  domain: ctx.domain,
                  uid: ctx.uid,
                };
                if (this.queue_provider) {
                  msgpack_encode(event, (e: Error, pkt: Buffer) => {
                    if (e) {
                      logerror(e);
                    } else {
                      this.queue_provider.instance().addjob(queuename, pkt, { retry: 0 }, () => {
                      }, (e: Error) => {
                        logerror(e);
                        this.queue_provider.error(e);
                      });
                    }
                  });
                }
              },
              report: this.queue_provider ?  (level: number, error: Error) => {
                const payload: ErrorPacket = {
                  module: modname,
                  function: pkt.cmd,
                  level,
                  error,
                };
                const epkt = msgpack.encode(payload);
                this.queue_provider.instance().addjob("hive-errors", epkt, { retry: 0 }, () => {}, (e: Error) => {
                  logerror(e);
                  this.queue_provider.error(e);
                });
              } : (level: number, error: Error) => {
                logerror(error);
              },
              logerror,
            };
            if (!asynced) {
              const func = impl as ProcessorFunction;
              try {
                pkt.args ? func(ctx, ...pkt.args) : func(ctx);
              } catch (e) {
                report_processor_error(ctx, pkt.cmd, 0, e, pkt.args);
              } finally {
                done();
              }
            } else {
              const func = impl as AsyncProcessorFunction;
              const r = pkt.args ? func(ctx, ...pkt.args) : func(ctx);
              r.then(result => {
                done();
                if (result !== undefined) {
                  msgpack_encode(result, (e: Error, buf: Buffer) => {
                    if (e) {
                      report_processor_error(ctx, "processor/asyncfunc/msgpack_encode", 0, e);
                    } else {
                      cache.setex(`results:${pkt.sn}`, 600, buf, (e: Error, _: any) => {
                        if (e) {
                          report_processor_error(ctx, "processor/asyncfunc/msgpack_encode/setex", 0, e);
                        }
                      });
                    }
                  });
                }
              }).catch(e => {
                done();
                report_processor_error(ctx, pkt.cmd, 0, e, pkt.args);
                msgpack_encode({ code: 500, msg: e.message }, (e: Error, buf: Buffer) => {
                  if (e) {
                    report_processor_error(ctx, "processor/asyncfunc-catch/msgpack_encode", 0, e);
                  } else {
                    cache.setex(`results:${pkt.sn}`, 600, buf, (e: Error, _: any) => {
                      if (e) {
                        report_processor_error(ctx, "processor/asyncfunc-catch/msgpack_encode/setex", 0, e);
                      }
                    });
                  }
                });
              });
            }
          }
        });
      } else {
        loginfo(pkt.cmd + " not found!");
      }
    }).bind(this));
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
  domain: string;
  uid: string;
  sn: string;
  data: any;
}

export interface BusinessEventContext extends Context {
  pool: Pool;
  queue_provider: QueueProvider;
  queuename: string;
  handler: BusinessEventHandlerFunction;
  loginfo: Function;
  logerror: Function;
  db?: PGClient;
}

export interface BusinessEventHandlerFunction {
  (ctx: BusinessEventContext, data: any): Promise<any>;
}

function report_timer_error(ctx: BusinessEventContext, fun: string, level: number, e: Error, callback: (() => void)) {
  ctx.logerror(e);
  const payload: ErrorPacket = {
    module: ctx.modname,
    function: fun,
    level: 0,
    error: e,
  };
  const pkt = msgpack.encode(payload);
  ctx.queue_provider.instance().addjob("hive-errors", pkt, { retry: 0 }, () => {
    callback();
  }, (e: Error) => {
    ctx.logerror(e);
    ctx.queue_provider.error(e);
    callback();
  });
}

function business_event_loop(ctx: BusinessEventContext) {
  const options = {
    timeout: 10,
    count: 1,
  };
  ctx.queue_provider.instance().getjob(ctx.queuename, options, jobs => {
    if (jobs.length > 0) {
      const job = jobs[0];
      const body = job.body as Buffer;
      msgpack_decode(body, (e: Error, pkt: BusinessEventPacket) => {
        if (e) {
          report_timer_error(ctx, "business_event_loop/getjob/msgpack_decode", 0, e, () => { setTimeout(business_event_loop, 1000, ctx); });
        } else {
          ctx.pool.connect((err: Error, db: PGClient, done: () => void) => {
            if (err) {
              report_timer_error(ctx, "business_event_loop/getjob/msgpack_decode/connect", 0, err, () => { setTimeout(business_event_loop, 1000, ctx); });
            } else {
              ctx.db = db;
              ctx.domain = pkt.domain;
              ctx.uid = pkt.uid;
              ctx.handler(ctx, pkt.data).then(result => {
                done();
                if (result !== undefined) {
                  msgpack_encode(result, (e: Error, buf: Buffer) => {
                    if (e) {
                      report_timer_error(ctx, "business_event_loop/getjob/msgpack_decode/connect/handle.then/msgpack_encode", 0, e, () => { setTimeout(business_event_loop, 1000, ctx); });
                    } else {
                      ctx.cache.setex(`results:${pkt.sn}`, 600, buf, (e: Error, _: any) => {
                        if (e) {
                          report_timer_error(ctx, "business_event_loop/getjob/msgpack_decode/connect/handle.then/msgpack_encode/setex", 0, e, () => { setTimeout(business_event_loop, 1000, ctx); });
                        } else {
                          setTimeout(business_event_loop, 0, ctx);
                        }
                      });
                    }
                  });
                } else {
                  msgpack_encode({ code: 500, msg: `${ctx.modname} 的 BusinessEventHandlerFunction 没有返回结果` }, (e: Error, buf: Buffer) => {
                    if (e) {
                      ctx.logerror(e);
                      report_timer_error(ctx, "business_event_loop/getjob/msgpack_decode/connect/handle.then/msgpack_encode", 0, e, () => { setTimeout(business_event_loop, 1000, ctx); });
                    } else {
                      ctx.cache.setex(`results:${pkt.sn}`, 600, buf, (e: Error, _: any) => {
                        if (e) {
                          report_timer_error(ctx, "business_event_loop/getjob/msgpack_decode/connect/handle.then/msgpack_encode/setex", 0, e, () => { setTimeout(business_event_loop, 1000, ctx); });
                        } else {
                          setTimeout(business_event_loop, 1000, ctx);
                        }
                      });
                    }
                  });
                }
              }).catch(e => {
                done();
                ctx.logerror(e);
                report_timer_error(ctx, "queue: " + ctx.queuename, 0, e, () => {
                  msgpack_encode({ code: 500, msg: e.message }, (e: Error, buf: Buffer) => {
                    if (e) {
                      report_timer_error(ctx, "business_event_loop/getjob/msgpack_decode/connect/handle.catch/msgpack_encode", 0, e, () => { setTimeout(business_event_loop, 1000, ctx); });
                    } else {
                      ctx.cache.setex(`results:${pkt.sn}`, 600, buf, (e: Error, _: any) => {
                        if (e) {
                          report_timer_error(ctx, "business_event_loop/getjob/msgpack_decode/connect/handle.catch/msgpack_encode/setex", 0, e, () => { setTimeout(business_event_loop, 1000, ctx); });
                        } else {
                          setTimeout(business_event_loop, 1000, ctx);
                        }
                      });
                    }
                  });
                });
              });
            }
          });
        }
      });
    } else {
      setTimeout(business_event_loop, 1000, ctx);
    }
  }, (e: Error) => {
    ctx.logerror(e);
    ctx.queue_provider.error(e);
    report_timer_error(ctx, "business_event_loop/getjob.error", 0, e, () => { setTimeout(business_event_loop, 1000, ctx); });
  });
}

export class BusinessEventListener {
  queuename: string;
  handler: BusinessEventHandlerFunction;
  constructor(queuename: string) {
    this.queuename = queuename;
  }

  public init(modname: string, pool: Pool, cache: RedisClient, loginfo: Function, logerror: Function, queue_provider: QueueProvider): void {
    const ctx: BusinessEventContext = {
      pool,
      cache,
      queue_provider,
      queuename: this.queuename,
      handler: this.handler,
      report: (level: number, error: Error) => {
        const payload: ErrorPacket = {
          module: modname,
          function: this.queuename,
          level,
          error,
        };
        const pkt = msgpack.encode(payload);
        queue_provider.instance().addjob("hive-errors", pkt, { retry: 0 }, () => {}, (e: Error) => {
          logerror(e);
          queue_provider.error(e);
        });
      },
      modname,
      loginfo,
      logerror,
      db: undefined,
      domain: undefined,
      uid: undefined,
      sn: undefined,
    };
    business_event_loop(ctx);
  }

  public onEvent(handler: BusinessEventHandlerFunction) {
    this.handler = handler;
  }
}

export interface Config {
  modname: string;
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
  loginfo?: ((...args: any[]) => void);
  logerror?: ((...args: any[]) => void);
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
    if (this.config.queueaddr) {
      const path = this.config.queueaddr.substring(this.config.queueaddr.indexOf("///") + 2, this.config.queueaddr.length);
      if (fs.existsSync(path)) {
        fs.unlinkSync(path); // make nanomsg happy
      }
    }
    const cache: RedisClient = createClient(this.config.cacheport ? this.config.cacheport : 6379, this.config.cachehost, { "return_buffers": true });
    const cacheAsync: RedisClient = bluebird.promisifyAll(cache) as RedisClient;
    const dbconfig = {
      host: this.config.dbhost,
      user: this.config.dbuser,
      database: this.config.database,
      password: this.config.dbpasswd,
      port: this.config.dbport ? this.config.dbport : 5432,
      min: 1, // min number of clients in the pool
      max: 2 * (this.processors.length + this.listeners.length), // max number of clients in the pool
      idleTimeoutMillis: 30000, // how long a client is allowed to remain idle before being closed
    };
    const pool = new Pool(dbconfig);

    if (this.config.queuehost) {
      const port = this.config.queueport ? this.config.queueport : 7711;
      const queue = new Disq({nodes: [`${this.config.queuehost}:${port}`]});
      this.server.init(this.config.modname, this.config.serveraddr, this.config.queueaddr, cacheAsync, this.config.loginfo || console.log, this.config.logerror || console.log, new QueueProvider([`${this.config.queuehost}:${port}`]));
      for (const processor of this.processors) {
        processor.init(this.config.modname, this.config.queueaddr, pool, cacheAsync, this.config.loginfo || console.log, this.config.logerror || console.log, new QueueProvider([`${this.config.queuehost}:${port}`]));
      }
      for (const listener of this.listeners) {
        listener.init(this.config.modname, pool, cacheAsync, this.config.loginfo || console.log, this.config.logerror || console.log, new QueueProvider([`${this.config.queuehost}:${port}`]));
      }
    } else {
      this.server.init(this.config.modname, this.config.serveraddr, this.config.queueaddr, cacheAsync, this.config.loginfo || console.log, this.config.logerror || console.log);

      for (const processor of this.processors) {
        processor.init(this.config.modname, this.config.queueaddr, pool, cacheAsync, this.config.loginfo || console.log, this.config.logerror || console.log);
      }
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

const fibs = [0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, 6765, 10946, 17711, 28657, 46368, 75025, 121393, 196418, 317811, 514229, 832040, 1346269, 2178309, 3524578, 5702887, 9227465, 14930352, 24157817, 39088169, 63245986, 102334155, 165580141, 267914296, 433494437, 701408733, 1134903170, 1836311903, 2971215073, 4807526976, 7778742049, 12586269025, 20365011074, 32951280099, 53316291173, 86267571272, 139583862445, 225851433717, 365435296162, 591286729879, 956722026041, 1548008755920, 2504730781961, 4052739537881, 6557470319842, 10610209857723, 17167680177565, 27777890035288, 44945570212853, 72723460248141, 117669030460994, 190392490709135, 308061521170129, 498454011879264, 806515533049393, 1304969544928657, 2111485077978050, 3416454622906707, 5527939700884757, 8944394323791464, 14472334024676220, 23416728348467690, 37889062373143900, 61305790721611590, 99194853094755490, 160500643816367070, 259695496911122600, 420196140727489660, 679891637638612200, 1100087778366102000, 1779979416004714200, 2880067194370816000, 4660046610375530000, 7540113804746346000, 12200160415121877000, 19740274219868226000, 31940434634990100000, 51680708854858320000, 83621143489848430000, 135301852344706740000, 218922995834555140000];

export function fib(n: number): number {
  return fibs[n];
}

export function fiball(n: number): number {
  return fib_iter(1, 0, 0, 1, n);
}

function timer_callback(cache: RedisClient, reply: string, rep: ((result: any) => void), retry: number, countdown: number) {
  cache.get(reply, (err: Error, result: Buffer) => {
    if (result) {
      msgpack_decode_async<any>(result).then(obj => {
        rep(obj);
      }).catch((e: Error) => {
        rep({
          code: 540,
          msg: e.message,
        });
      });
    } else if (countdown === 0) {
      rep({
        code: 504,
        msg: "Request Timeout",
      });
    } else {
      setTimeout(timer_callback, fib(retry - countdown) * 1000, cache, reply, rep, retry, countdown - 1);
    }
  });
}

export function waiting(ctx: Context, rep: ((result: Result<any>) => void), sn: string = ctx.sn, retry: number = 7) {
  setTimeout(timer_callback, 100, ctx.cache, `results:${sn}`, rep, retry + 1, retry);
}

export function wait_for_response(cache: RedisClient, reply: string, rep: ((result: Result<any>) => void), retry: number = 7) {
  setTimeout(timer_callback, 100, cache, reply, rep, retry + 1, retry);
}

export function set_for_response(cache: RedisClient, key: string, value: Result<any>, timeout: number = 30): Promise<any> {
  return new Promise((resolve, reject) => {
    msgpack_encode_async(value).then(buf => {
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
      msgpack_decode_async<any>(result).then(obj => {
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
      setTimeout(async_timer_callback, fib(retry - countdown) * 1000, cache, reply, resolve, reject, retry, countdown - 1);
    }
  });
}

export function waitingAsync(ctx: Context, sn: string = ctx.sn, retry: number = 7): Promise<Result<any>> {
  return new Promise<Result<any>>((resolve, reject) => {
    setTimeout(async_timer_callback, 100, ctx.cache, `results:${sn}`, resolve, reject, retry + 1, retry);
  });
}

export function rpc<T>(domain: string, addr: string, uid: string, cb: ((e: Error, result: Result<T>) => void), fun: string, ...args: any[]) {
  const host = process.env["RPC-HOST"] || "127.0.0.1";
  const port = process.env["GATEWAY-" + (domain.toUpperCase()) + "-PORT"] || 8000;
  const path = process.env["RPC-PATH"] || "/";
  const openid = uid;
  const mod = Object.keys(process.env).filter(x => process.env[x] === addr);

  const data: Buffer = msgpack.encode({
    "mod": mod[0].toLowerCase(),
    "fun": fun,
    "arg": [...args],
    "ctx": {
      "wxuser": openid,
    },
  });

  const options = {
    hostname: host,
    port: port,
    path: path,
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      "Content-Length": data.length,
    }
  };

  const req = http.request(options, (rep) => {
    const chunks: Buffer[] = [];
    rep.on("data", (chunk) => {
      const buf = chunk as Buffer;
      chunks.push(buf);
    });
    rep.on("end", () => {
      const buffer = Buffer.concat(chunks);
      const data = msgpack.decode(buffer);
      cb(null, data);
    });
  });
  req.on("error", (e) => {
    cb(e, null);
  });
  // write data to request body
  req.write(data);
  req.end();
}

export function rpcAsync<T>(domain: string, addr: string, uid: string, fun: string, ...args: any[]) : Promise<Result<T>> {
  const host = process.env["RPC-HOST"] || "127.0.0.1";
  const port = process.env["GATEWAY-" + (domain.toUpperCase()) + "-PORT"] || 8000;
  const path = process.env["RPC-PATH"] || "/";
  const openid = uid;
  const mods: string[] = Object.keys(process.env).filter(x => process.env[x] === addr);

  if (mods === null || mods.length === 0) {
    return Promise.resolve({ code: 404, msg: `Module with address(${addr}) not found!`});
  }

  const data: Buffer = msgpack.encode({
    "mod": mods[0].toLowerCase(),
    "fun": fun,
    "arg": [...args],
    "ctx": {
      "wxuser": openid,
    },
  });

  const options = {
    hostname: host,
    port: port,
    path: path,
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      "Content-Length": data.length,
    }
  };

  const p = new Promise<any>((resolve, reject) => {
    const req = http.request(options, (rep) => {
      const chunks: Buffer[] = [];
      rep.on("data", (chunk) => {
        const buf = chunk as Buffer;
        chunks.push(buf);
      });
      rep.on("end", () => {
        const buffer = Buffer.concat(chunks);
        const data = msgpack.decode(buffer);
        resolve(data);
      });
      rep.destroy();
    });
    req.on("error", (e) => {
      reject(e);
    });
    // write data to request body
    req.write(data);
    req.end();
  });
  return p;
}

export interface Paging<T> {
  count: number;
  offset: number;
  limit: number;
  data: T[];
}

export function msgpack_encode_async(obj: any): Promise<Buffer> {
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

export function msgpack_encode(obj: any, cb: ((e: Error, buf: Buffer) => void)) {
  const buf: Buffer = msgpack.encode(obj);
  if (buf.length > 1024) {
    zlib.deflate(buf, (e: Error, newbuf: Buffer) => {
      cb(e, newbuf);
    });
  } else {
    cb(null, buf);
  }
}

export function msgpack_decode_async<T>(buf: Buffer): Promise<T> {
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

export function msgpack_decode<T>(buf: Buffer, cb: (e: Error, obj: T) => void) {
  if (buf[0] === 0x78 && buf[1] === 0x9c) {
    zlib.inflate(buf, (e: Error, newbuf: Buffer) => {
      if (e) {
        cb(e, null);
      } else {
        try {
          const result: T = msgpack.decode(buf) as T;
          cb(null, result);
        } catch (e) {
          cb(e, null);
        }
      }
    });
  } else {
    try {
      const result: T = msgpack.decode(buf) as T;
      cb(null, result);
    } catch (e) {
      cb(e, null);
    }
  }
}
