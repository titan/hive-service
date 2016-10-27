"use strict";
const msgpack = require('msgpack-lite');
const nanomsg_1 = require('nanomsg');
const fs = require("fs");
const pg_1 = require('pg');
const redis_1 = require('redis');
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
            const pkt = msgpack.decode(buf);
            const ctx = pkt.ctx;
            ctx.queue = _self.pub;
            ctx.cache = cache;
            const fun = pkt.fun;
            const args = pkt.args;
            if (_self.permissions.has(fun) && _self.permissions.get(fun).get(ctx.domain)) {
                const func = _self.functions.get(fun);
                if (args != null) {
                    func(ctx, function (result) {
                        _self.rep.send(msgpack.encode(result));
                    }, ...args);
                }
                else {
                    func(ctx, function (result) {
                        _self.rep.send(msgpack.encode(result));
                    });
                }
            }
            else {
                _self.rep.send(msgpack.encode({ code: 403, msg: "Forbidden" }));
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
    constructor() {
        this.functions = new Map();
    }
    init(queueaddr, pool, cache) {
        this.queueaddr = queueaddr;
        this.sock = nanomsg_1.socket("sub");
        this.sock.connect(this.queueaddr);
        const _self = this;
        this.sock.on('data', (buf) => {
            const pkt = msgpack.decode(buf);
            if (_self.functions.has(pkt.cmd)) {
                pool.connect().then(db => {
                    const func = _self.functions.get(pkt.cmd);
                    if (pkt.args) {
                        func(db, cache, () => {
                            db.release();
                        }, ...pkt.args);
                    }
                    else {
                        func(db, cache, () => {
                            db.release();
                        });
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
    call(cmd, impl) {
        this.functions.set(cmd, impl);
    }
}
exports.Processor = Processor;
class Service {
    constructor(config) {
        this.config = config;
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
