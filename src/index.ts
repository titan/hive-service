import * as msgpack from 'msgpack-lite';
import * as crypto from "crypto";
import { Socket, socket } from 'nanomsg';
import * as fs from "fs";
import * as ip from 'ip';
import { Pool, Client as PGClient } from 'pg';
import { createClient, RedisClient} from 'redis';

export interface CmdPacket {
  cmd: string,
  args: any[]
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
  serveraddr: string;
  queueaddr: string;
  rep: Socket;
  pub: Socket;

  functions: Map<string, ServerFunction>;
  permissions: Map<string, Map<string, boolean>>; // {function => { domain => permission }}

  constructor() {
    this.functions = new Map<string, ServerFunction>();
    this.permissions = new Map<string, Map<string, boolean>>();
  }

  public init(serveraddr: string, queueaddr: string, cache: RedisClient): void {
    this.serveraddr = serveraddr;
    this.queueaddr = queueaddr;
    this.rep = socket("rep");
    this.rep.bind(this.serveraddr);
    this.pub = socket("pub");
    this.pub.bind(this.queueaddr);

    const _self = this;

    this.rep.on("data", function (buf: NodeBuffer) {
      const data = msgpack.decode(buf);
      const pkt = data.pkt;
      const sn = data.sn;
      const ctx: ServerContext = pkt.ctx; /* Domain, IP, User */
      ctx.cache = cache;
      const fun: string = pkt.fun;
      const args: any[] = pkt.args;
      if (_self.permissions.has(fun) && _self.permissions.get(fun).get(ctx.domain)) {
        const func: ServerFunction = _self.functions.get(fun);
        if (args != null) {
          ctx.publish = (pkt: CmdPacket) => _self.pub.send(msgpack.encode(pkt));
          func(ctx, function(result) {
            const payload = msgpack.encode(result);
            _self.rep.send(msgpack.encode({ sn, payload }));
          }, ...args);
        } else {
          func(ctx, function(result) {
            const payload = msgpack.encode(result);
            _self.rep.send(msgpack.encode({ sn, payload }));
          });
        }
      } else {
        const payload = msgpack.encode({code: 403, msg: "Forbidden"});
        _self.rep.send(msgpack.encode({ sn, payload }));
      }
    });
  }

  public call(fun: string, permissions: Permission[], impl: ServerFunction): void {
    this.functions.set(fun, impl);
    this.permissions.set(fun, new Map(permissions));
  }
}

export interface ProcessorFunction {
  (db: PGClient, cache: RedisClient, done: (() => void), ...args: any[]): void;
}

export class Processor {
  queueaddr: string;
  sock: Socket;
  functions: Map<string, ProcessorFunction>;

  constructor() {
    this.functions = new Map<string, ProcessorFunction>();
  }

  public init(queueaddr: string, pool: Pool, cache: RedisClient): void {
    this.queueaddr = queueaddr;
    this.sock = socket("sub");
    this.sock.connect(this.queueaddr);
    const _self = this;
    this.sock.on('data', (buf: NodeBuffer) => {
      const pkt: CmdPacket = msgpack.decode(buf);
      if (_self.functions.has(pkt.cmd)) {
        pool.connect().then(db => {
          const func = _self.functions.get(pkt.cmd);
          if (pkt.args) {
            func(db, cache, () => {
              db.release();
            }, ...pkt.args);
          } else {
            func(db, cache, () => {
              db.release();
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

  public call(cmd: string, impl: ProcessorFunction): void {
    this.functions.set(cmd, impl);
  }
}

export interface Config {
  serveraddr: string;
  queueaddr: string;
  dbhost: string,
  dbuser: string,
  dbport?: number,
  database: string,
  dbpasswd: string,
  cachehost: string,
  cacheport?: number
}

export class Service {
  config: Config;
  server: Server;
  processors: Processor[];

  constructor(config: Config) {
    this.config = config;
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

    const cache = createClient(this.config.cacheport ? this.config.cacheport : 6379, this.config.cachehost);
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

    this.server.init(this.config.serveraddr, this.config.queueaddr, cache);
    for (const processor of this.processors) {
      processor.init(this.config.queueaddr, pool, cache);
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

function timer_callback(cache: RedisClient, reply: string, rep: ((result: any) => void), countdown: number) {
  cache.get(reply, (err: Error, result) => {
    if (result) {
      rep(JSON.parse(result));
    } else if (countdown === 0) {
      rep({
        code: 408,
        msg: "Request Timeout"
      });
    } else {
      setTimeout(timer_callback, fib(8 - countdown) * 1000, cache, reply, rep, countdown - 1);
    }
  });
}

export function wait_for_response(cache: RedisClient, reply: string, rep: ((result: any) => void)) {
  setTimeout(timer_callback, 500, cache, reply, rep, 7);
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
        resolve(data["payload"]);
      } else {
        reject(new Error("Invalid calling sequence number"));
      }
      req.shutdown(addr);
    });
    req.send(msgpack.encode({ sn, payload: msgpack.encode(params)}));
  });
  return p;
}
