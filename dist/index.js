"use strict";
const msgpack = require("msgpack-lite");
const crypto = require("crypto");
const nanomsg_1 = require("nanomsg");
const fs = require("fs");
const ip = require("ip");
const pg_1 = require("pg");
const redis_1 = require("redis");
class Server {
    constructor() {
        this.functions = new Map();
        this.permissions = new Map();
    }
    init(serveraddr, queueaddr, cache) {
        this.serveraddr = serveraddr;
        this.queueaddr = queueaddr;
        this.rep = nanomsg_1.socket("rep");
        this.rep.bind(this.serveraddr);
        this.pub = nanomsg_1.socket("pub");
        this.pub.bind(this.queueaddr);
        const _self = this;
        this.rep.on("data", function (buf) {
            const data = msgpack.decode(buf);
            const pkt = data.pkt;
            const sn = data.sn;
            const ctx = pkt.ctx;
            ctx.cache = cache;
            const fun = pkt.fun;
            const args = pkt.args;
            if (_self.permissions.has(fun) && _self.permissions.get(fun).get(ctx.domain)) {
                const func = _self.functions.get(fun);
                if (args != null) {
                    ctx.publish = (pkt) => _self.pub.send(msgpack.encode(pkt));
                    func(ctx, function (result) {
                        const payload = msgpack.encode(result);
                        _self.rep.send(msgpack.encode({ sn, payload }));
                    }, ...args);
                }
                else {
                    func(ctx, function (result) {
                        const payload = msgpack.encode(result);
                        _self.rep.send(msgpack.encode({ sn, payload }));
                    });
                }
            }
            else {
                const payload = msgpack.encode({ code: 403, msg: "Forbidden" });
                _self.rep.send(msgpack.encode({ sn, payload }));
            }
        });
    }
    call(fun, permissions, impl) {
        this.functions.set(fun, impl);
        this.permissions.set(fun, new Map(permissions));
    }
}
exports.Server = Server;
class Processor {
    constructor(subqueueaddr) {
        this.functions = new Map();
        this.subprocessors = [];
        if (subqueueaddr) {
            this.subqueueaddr = subqueueaddr;
            const path = subqueueaddr.substring(subqueueaddr.indexOf("///") + 2, subqueueaddr.length);
            if (fs.existsSync(path)) {
                fs.unlinkSync(path);
            }
        }
    }
    init(queueaddr, pool, cache) {
        this.queueaddr = queueaddr;
        this.sock = nanomsg_1.socket("sub");
        this.sock.connect(this.queueaddr);
        if (this.subqueueaddr) {
            this.pub = nanomsg_1.socket("pub");
            this.pub.bind(this.subqueueaddr);
            for (const subprocessor of this.subprocessors) {
                subprocessor.init(this.subqueueaddr, pool, cache);
            }
        }
        const _self = this;
        this.sock.on("data", (buf) => {
            const pkt = msgpack.decode(buf);
            if (_self.functions.has(pkt.cmd)) {
                pool.connect().then(db => {
                    const func = _self.functions.get(pkt.cmd);
                    let ctx = {
                        db,
                        cache,
                        done: () => { db.release(); },
                        publish: (pkt) => _self.pub ? _self.pub.send(msgpack.encode(pkt)) : undefined,
                    };
                    if (pkt.args) {
                        func(ctx, ...pkt.args);
                    }
                    else {
                        func(ctx);
                    }
                }).catch(e => {
                    console.log("DB connection error " + e.stack);
                });
            }
            else {
                console.error(pkt.cmd + " not found!");
            }
        });
    }
    registerSubProcessor(processor) {
        this.subprocessors.push(processor);
    }
    call(cmd, impl) {
        this.functions.set(cmd, impl);
    }
}
exports.Processor = Processor;
class Service {
    constructor(config) {
        this.config = config;
        this.processors = [];
    }
    registerServer(server) {
        this.server = server;
    }
    registerProcessor(processor) {
        this.processors.push(processor);
    }
    run() {
        const path = this.config.queueaddr.substring(this.config.queueaddr.indexOf("///") + 2, this.config.queueaddr.length);
        if (fs.existsSync(path)) {
            fs.unlinkSync(path);
        }
        const cache = redis_1.createClient(this.config.cacheport ? this.config.cacheport : 6379, this.config.cachehost);
        const dbconfig = {
            host: this.config.dbhost,
            user: this.config.dbuser,
            database: this.config.database,
            password: this.config.dbpasswd,
            port: this.config.dbport ? this.config.dbport : 5432,
            min: 1,
            max: 2 * this.processors.length,
            idleTimeoutMillis: 30000,
        };
        const pool = new pg_1.Pool(dbconfig);
        this.server.init(this.config.serveraddr, this.config.queueaddr, cache);
        for (const processor of this.processors) {
            processor.init(this.config.queueaddr, pool, cache);
        }
    }
}
exports.Service = Service;
function async_serial(ps, scb, fcb) {
    _async_serial(ps, [], scb, fcb);
}
exports.async_serial = async_serial;
function _async_serial(ps, acc, scb, fcb) {
    if (ps.length === 0) {
        scb(acc);
    }
    else {
        let p = ps.shift();
        p.then(val => {
            acc.push(val);
            _async_serial(ps, acc, scb, fcb);
        }).catch((e) => {
            fcb(e);
        });
    }
}
function async_serial_ignore(ps, cb) {
    _async_serial_ignore(ps, [], cb);
}
exports.async_serial_ignore = async_serial_ignore;
function _async_serial_ignore(ps, acc, cb) {
    if (ps.length === 0) {
        cb(acc);
    }
    else {
        let p = ps.shift();
        p.then(val => {
            acc.push(val);
            _async_serial_ignore(ps, acc, cb);
        }).catch((e) => {
            _async_serial_ignore(ps, acc, cb);
        });
    }
}
function fib_iter(a, b, p, q, n) {
    if (n === 0) {
        return b;
    }
    if (n % 2 === 0) {
        return fib_iter(a, b, p * p + q * q, 2 * p * q + q * q, n / 2);
    }
    return fib_iter(a * p + a * q + b * q, b * p + a * q, p, q, n - 1);
}
function fib(n) {
    return fib_iter(1, 0, 0, 1, n);
}
exports.fib = fib;
function timer_callback(cache, reply, rep, countdown) {
    cache.get(reply, (err, result) => {
        if (result) {
            rep(JSON.parse(result));
        }
        else if (countdown === 0) {
            rep({
                code: 408,
                msg: "Request Timeout"
            });
        }
        else {
            setTimeout(timer_callback, fib(8 - countdown) * 1000, cache, reply, rep, countdown - 1);
        }
    });
}
function wait_for_response(cache, reply, rep) {
    setTimeout(timer_callback, 500, cache, reply, rep, 7);
}
exports.wait_for_response = wait_for_response;
function rpc(domain, addr, uid, fun, ...args) {
    const p = new Promise(function (resolve, reject) {
        let a = [];
        if (args != null) {
            a = [...args];
        }
        const params = {
            ctx: {
                domain: domain,
                ip: ip.address(),
                uid: uid
            },
            fun: fun,
            args: a
        };
        const sn = crypto.randomBytes(64).toString("base64");
        const req = nanomsg_1.socket("req");
        req.connect(addr);
        req.on("data", (msg) => {
            const data = msgpack.decode(msg);
            if (sn === data["sn"]) {
                resolve(data["payload"]);
            }
            else {
                reject(new Error("Invalid calling sequence number"));
            }
            req.shutdown(addr);
        });
        req.send(msgpack.encode({ sn, payload: msgpack.encode(params) }));
    });
    return p;
}
exports.rpc = rpc;
