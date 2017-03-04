"use strict";
var __assign = (this && this.__assign) || Object.assign || function(t) {
    for (var s, i = 1, n = arguments.length; i < n; i++) {
        s = arguments[i];
        for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
            t[p] = s[p];
    }
    return t;
};
const msgpack = require("msgpack-lite");
const crypto = require("crypto");
const nanomsg_1 = require("nanomsg");
const fs = require("fs");
const ip = require("ip");
const bluebird = require("bluebird");
const zlib = require("zlib");
const pg_1 = require("pg");
const redis_1 = require("redis");
const hive_disque_1 = require("hive-disque");
function server_msgpack(sn, obj, callback) {
    const payload = msgpack.encode(obj);
    if (payload.length > 1024) {
        zlib.deflate(payload, (e, newbuf) => {
            if (e) {
                callback(msgpack.encode({ sn, payload }));
            }
            else {
                callback(msgpack.encode({ sn, payload: newbuf }));
            }
        });
    }
    else {
        callback(msgpack.encode({ sn, payload }));
    }
}
class Server {
    constructor() {
        this.functions = new Map();
        this.permissions = new Map();
    }
    init(modname, serveraddr, queueaddr, cache, loginfo, logerror, queue) {
        this.modname = modname;
        this.queueaddr = queueaddr;
        this.rep = nanomsg_1.socket("rep");
        this.rep.bind(serveraddr);
        this.pub = nanomsg_1.socket("pub");
        this.pub.bind(this.queueaddr);
        const lastnumber = parseInt(serveraddr[serveraddr.length - 1]) + 1;
        const newaddr = serveraddr.substr(0, serveraddr.length - 1) + lastnumber.toString();
        this.pair = nanomsg_1.socket("pair");
        this.pair.bind(newaddr);
        this.queue = queue;
        this.loginfo = loginfo;
        this.logerror = logerror;
        const _self = this;
        for (const sock of [this.pair, this.rep]) {
            sock.on("data", function (buf) {
                const data = msgpack.decode(buf);
                const pkt = data.pkt;
                const sn = data.sn;
                const ctx = {
                    domain: undefined,
                    ip: undefined,
                    uid: undefined,
                    cache: undefined,
                    publish: undefined,
                    push: undefined,
                    report: undefined,
                    sn,
                };
                for (const key in pkt.ctx) {
                    ctx[key] = pkt.ctx[key];
                }
                ctx.cache = cache;
                const fun = pkt.fun;
                const args = pkt.args;
                if (_self.permissions.has(fun) && _self.permissions.get(fun).get(ctx.domain)) {
                    const [asynced, impl] = _self.functions.get(fun);
                    ctx.publish = (pkt) => _self.pub.send(msgpack.encode(__assign({}, pkt, { sn, domain: ctx.domain, uid: ctx.uid })));
                    ctx.push = (queuename, data, qsn) => {
                        const event = {
                            sn: qsn || sn,
                            data,
                            domain: ctx.domain,
                            uid: ctx.uid
                        };
                        if (_self.queue) {
                            msgpack_encode(event, (e, pkt) => {
                                if (e) {
                                    logerror(e);
                                }
                                else {
                                    _self.queue.addjob(queuename, pkt, { retry: 1, ttl: 86400 * 3 }, () => {
                                    }, (e) => {
                                        logerror(e);
                                    });
                                }
                            });
                        }
                    };
                    ctx.report = _self.queue ?
                        (level, error) => {
                            const payload = {
                                module: _self.modname,
                                function: fun,
                                level,
                                error
                            };
                            const pkt = msgpack.encode(payload);
                            _self.queue.addjob("hive-errors", pkt, () => { }, (e) => {
                                logerror(e);
                            });
                        } :
                        (level, error) => {
                        };
                    if (!asynced) {
                        const func = impl;
                        try {
                            args ?
                                func(ctx, (result) => {
                                    server_msgpack(sn, result, (buf) => { sock.send(buf); });
                                }, ...args) :
                                func(ctx, (result) => {
                                    server_msgpack(sn, result, (buf) => { sock.send(buf); });
                                });
                        }
                        catch (e) {
                            logerror(e);
                            const payload = msgpack.encode({ code: 500, msg: e.message });
                            msgpack_encode({ sn, payload }, (e, pkt) => {
                                if (e) {
                                    logerror(e);
                                }
                                else {
                                    sock.send(pkt);
                                }
                            });
                        }
                    }
                    else {
                        const func = impl;
                        func(ctx, ...args).then(result => {
                            server_msgpack(sn, result, (buf) => { sock.send(buf); });
                        }).catch(e => {
                            logerror(e);
                            const payload = msgpack.encode({ code: 500, msg: e.message });
                            msgpack_encode({ sn, payload }, (e, pkt) => {
                                if (e) {
                                    logerror(e);
                                }
                                else {
                                    sock.send(pkt);
                                }
                            });
                        });
                    }
                }
                else {
                    const payload = msgpack.encode({ code: 403, msg: "Forbidden" });
                    sock.send(msgpack.encode({ sn, payload }));
                }
            });
        }
    }
    call(fun, permissions, name, description, impl) {
        this.functions.set(fun, [false, impl]);
        this.permissions.set(fun, new Map(permissions));
    }
    callAsync(fun, permissions, name, description, impl) {
        this.functions.set(fun, [true, impl]);
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
    init(modname, queueaddr, pool, cache, loginfo, logerror, queue) {
        this.modname = modname;
        this.queueaddr = queueaddr;
        this.sock = nanomsg_1.socket("sub");
        this.sock.connect(this.queueaddr);
        this.queue = queue;
        this.loginfo = loginfo;
        this.logerror = logerror;
        if (this.subqueueaddr) {
            this.pub = nanomsg_1.socket("pub");
            this.pub.bind(this.subqueueaddr);
            for (const subprocessor of this.subprocessors) {
                subprocessor.init(modname, this.subqueueaddr, pool, cache, loginfo, logerror, queue);
            }
        }
        const _self = this;
        this.sock.on("data", (buf) => {
            const pkt = msgpack.decode(buf);
            if (_self.functions.has(pkt.cmd)) {
                const [asynced, impl] = _self.functions.get(pkt.cmd);
                pool.connect().then(db => {
                    const ctx = {
                        db,
                        cache,
                        domain: pkt.domain,
                        uid: pkt.uid,
                        done: !asynced ? (result) => {
                            if (result !== undefined) {
                                msgpack_encode_async(result).then(buf => {
                                    cache.setex(`results:${pkt.sn}`, 600, buf, (e, _) => {
                                        if (e) {
                                            logerror(e);
                                        }
                                    });
                                }).catch(e => {
                                    logerror(e);
                                });
                            }
                        } : undefined,
                        publish: (pkt) => _self.pub ? _self.pub.send(msgpack.encode(pkt)) : undefined,
                        report: _self.queue ?
                            (level, error) => {
                                const payload = {
                                    module: _self.modname,
                                    function: pkt.cmd,
                                    level,
                                    error
                                };
                                const epkt = msgpack.encode(payload);
                                _self.queue.addjob("hive-errors", epkt, () => { }, (e) => {
                                    this.logerror(e);
                                });
                            } :
                            (level, error) => {
                                this.logerror(error);
                            },
                    };
                    if (!asynced) {
                        const func = impl;
                        try {
                            func(ctx, ...pkt.args);
                        }
                        catch (e) {
                            this.logerror(e);
                        }
                        finally {
                            db.release();
                        }
                    }
                    else {
                        const func = impl;
                        func(ctx, ...pkt.args).then(result => {
                            db.release();
                            if (result !== undefined) {
                                msgpack_encode_async(result).then(buf => {
                                    cache.setex(`results:${pkt.sn}`, 600, buf, (e, _) => {
                                        if (e) {
                                            this.logerror(e);
                                        }
                                    });
                                }).catch(e => {
                                    this.logerror(e);
                                });
                            }
                        }).catch(e => {
                            db.release();
                            this.logerror(e);
                            msgpack_encode_async({ code: 500, msg: e.message }).then(buf => {
                                cache.setex(`results:${pkt.sn}`, 600, buf, (e, _) => {
                                    if (e) {
                                        this.logerror(e);
                                    }
                                });
                            }).catch(e => {
                                this.logerror(e);
                            });
                        });
                    }
                }).catch(e => {
                    this.logerror(e);
                });
            }
            else {
                this.loginfo(pkt.cmd + " not found!");
            }
        });
    }
    registerSubProcessor(processor) {
        this.subprocessors.push(processor);
    }
    call(cmd, impl) {
        this.functions.set(cmd, [false, impl]);
    }
    callAsync(cmd, impl) {
        this.functions.set(cmd, [true, impl]);
    }
}
exports.Processor = Processor;
function on_event_timer(ctx) {
    const options = {
        timeout: 10,
        count: 1,
    };
    ctx.queue.getjob(ctx.queuename, options, jobs => {
        if (jobs.length > 0) {
            const job = jobs[0];
            const body = job.body;
            msgpack_decode_async(body).then((pkt) => {
                ctx.pool.connect().then(db => {
                    ctx.db = db;
                    ctx.domain = pkt.domain;
                    ctx.uid = pkt.uid;
                    ctx.handler(ctx, pkt.data).then(result => {
                        db.release();
                        if (result !== undefined) {
                            msgpack_encode_async(result).then(buf => {
                                ctx.cache.setex(`results:${pkt.sn}`, 600, buf, (e, _) => {
                                    if (e) {
                                        ctx.logerror(e);
                                    }
                                    ctx.queue.ackjob(job.id, () => {
                                        setTimeout(on_event_timer, 0, ctx);
                                    }, (e) => {
                                        ctx.logerror(e);
                                        const payload = {
                                            module: ctx.modname,
                                            function: "on_event_timer",
                                            level: 0,
                                            error: e,
                                        };
                                        const pkt = msgpack.encode(payload);
                                        ctx.queue.addjob("hive-errors", pkt, () => { }, (e) => {
                                            ctx.logerror(e);
                                        });
                                        setTimeout(on_event_timer, 0, ctx);
                                    });
                                });
                            }).catch(e => {
                                ctx.logerror(e);
                                ctx.queue.ackjob(job.id, () => {
                                    setTimeout(on_event_timer, 0, ctx);
                                }, (e) => {
                                    ctx.logerror(e);
                                    const payload = {
                                        module: ctx.modname,
                                        function: "on_event_timer",
                                        level: 0,
                                        error: e,
                                    };
                                    const pkt = msgpack.encode(payload);
                                    ctx.queue.addjob("hive-errors", pkt, () => { }, (e) => {
                                        ctx.logerror(e);
                                    });
                                    setTimeout(on_event_timer, 0, ctx);
                                });
                            });
                        }
                    }).catch(e => {
                        db.release();
                        msgpack_encode_async({ code: 500, msg: e.message }).then(buf => {
                            ctx.cache.setex(`results:${pkt.sn}`, 600, buf, (e, _) => {
                                if (e) {
                                    ctx.logerror(e);
                                }
                                setTimeout(on_event_timer, 1000, ctx);
                            });
                        }).catch(e => {
                            ctx.logerror(e);
                            setTimeout(on_event_timer, 1000, ctx);
                        });
                    });
                }).catch(e => {
                    ctx.logerror(e);
                    setTimeout(on_event_timer, 1000, ctx);
                });
            }).catch(e => {
                ctx.logerror(e);
                ctx.queue.ackjob(job.id, () => {
                    setTimeout(on_event_timer, 1000, ctx);
                }, (e) => {
                    ctx.logerror(e);
                    const payload = {
                        module: ctx.modname,
                        function: "on_event_timer",
                        level: 0,
                        error: e,
                    };
                    const pkt = msgpack.encode(payload);
                    ctx.queue.addjob("hive-errors", pkt, () => { }, (e) => {
                        ctx.logerror(e);
                    });
                    setTimeout(on_event_timer, 1000, ctx);
                });
            });
        }
        else {
            setTimeout(on_event_timer, 1000, ctx);
        }
    }, (e) => {
        ctx.logerror(e);
        setTimeout(on_event_timer, 1000, ctx);
    });
}
class BusinessEventListener {
    constructor(queuename) {
        this.queuename = queuename;
    }
    init(modname, pool, cache, loginfo, logerror, queue) {
        const ctx = {
            pool,
            cache,
            queue,
            queuename: this.queuename,
            handler: this.handler,
            report: (level, error) => {
                const payload = {
                    module: modname,
                    function: this.queuename,
                    level,
                    error
                };
                const pkt = msgpack.encode(payload);
                queue.addjob("hive-errors", pkt, () => { }, (e) => {
                    logerror(e);
                });
            },
            modname,
            loginfo,
            logerror,
            db: undefined,
            domain: undefined,
            uid: undefined,
        };
        setTimeout(on_event_timer, 1000, ctx);
    }
    onEvent(handler) {
        this.handler = handler;
    }
}
exports.BusinessEventListener = BusinessEventListener;
class Service {
    constructor(config) {
        this.config = config;
        this.processors = [];
        this.listeners = [];
    }
    registerServer(server) {
        this.server = server;
    }
    registerProcessor(processor) {
        this.processors.push(processor);
    }
    registerEventListener(listener) {
        this.listeners.push(listener);
    }
    run() {
        const path = this.config.queueaddr.substring(this.config.queueaddr.indexOf("///") + 2, this.config.queueaddr.length);
        if (fs.existsSync(path)) {
            fs.unlinkSync(path);
        }
        const cache = redis_1.createClient(this.config.cacheport ? this.config.cacheport : 6379, this.config.cachehost, { "return_buffers": true });
        const cacheAsync = bluebird.promisifyAll(cache);
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
        const pool = new pg_1.Pool(this.config.loginfo ? __assign({}, dbconfig, { log: this.config.loginfo }) : dbconfig);
        if (this.config.queuehost) {
            const port = this.config.queueport ? this.config.queueport : 7711;
            const queue = new hive_disque_1.Disq({ nodes: [`${this.config.queuehost}:${port}`] });
            this.server.init(this.config.modname, this.config.serveraddr, this.config.queueaddr, cacheAsync, this.config.loginfo || console.log, this.config.logerror || console.log, queue);
            for (const processor of this.processors) {
                processor.init(this.config.modname, this.config.queueaddr, pool, cacheAsync, this.config.loginfo || console.log, this.config.logerror || console.log, queue);
            }
            for (const listener of this.listeners) {
                listener.init(this.config.modname, pool, cacheAsync, this.config.loginfo || console.log, this.config.logerror || console.log, queue);
            }
        }
        else {
            this.server.init(this.config.modname, this.config.serveraddr, this.config.queueaddr, cacheAsync, this.config.loginfo || console.log, this.config.logerror || console.log);
            for (const processor of this.processors) {
                processor.init(this.config.modname, this.config.queueaddr, pool, cacheAsync, this.config.loginfo || console.log, this.config.logerror || console.log);
            }
        }
    }
}
exports.Service = Service;
function fib_iter(a, b, p, q, n) {
    if (n === 0) {
        return b;
    }
    if (n % 2 === 0) {
        return fib_iter(a, b, p * p + q * q, 2 * p * q + q * q, n / 2);
    }
    return fib_iter(a * p + a * q + b * q, b * p + a * q, p, q, n - 1);
}
const fibs = [0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, 6765, 10946, 17711, 28657, 46368, 75025, 121393, 196418, 317811, 514229, 832040, 1346269, 2178309, 3524578, 5702887, 9227465, 14930352, 24157817, 39088169, 63245986, 102334155, 165580141, 267914296, 433494437, 701408733, 1134903170, 1836311903, 2971215073, 4807526976, 7778742049, 12586269025, 20365011074, 32951280099, 53316291173, 86267571272, 139583862445, 225851433717, 365435296162, 591286729879, 956722026041, 1548008755920, 2504730781961, 4052739537881, 6557470319842, 10610209857723, 17167680177565, 27777890035288, 44945570212853, 72723460248141, 117669030460994, 190392490709135, 308061521170129, 498454011879264, 806515533049393, 1304969544928657, 2111485077978050, 3416454622906707, 5527939700884757, 8944394323791464, 14472334024676220, 23416728348467690, 37889062373143900, 61305790721611590, 99194853094755490, 160500643816367070, 259695496911122600, 420196140727489660, 679891637638612200, 1100087778366102000, 1779979416004714200, 2880067194370816000, 4660046610375530000, 7540113804746346000, 12200160415121877000, 19740274219868226000, 31940434634990100000, 51680708854858320000, 83621143489848430000, 135301852344706740000, 218922995834555140000];
function fib(n) {
    return fibs[n];
}
exports.fib = fib;
function fiball(n) {
    return fib_iter(1, 0, 0, 1, n);
}
exports.fiball = fiball;
function timer_callback(cache, reply, rep, retry, countdown) {
    cache.get(reply, (err, result) => {
        if (result) {
            msgpack_decode_async(result).then(obj => {
                rep(obj);
            }).catch((e) => {
                rep({
                    code: 540,
                    msg: e.message
                });
            });
        }
        else if (countdown === 0) {
            rep({
                code: 504,
                msg: "Request Timeout"
            });
        }
        else {
            setTimeout(timer_callback, fib(retry - countdown) * 1000, cache, reply, rep, retry, countdown - 1);
        }
    });
}
function waiting(ctx, rep, retry = 7) {
    setTimeout(timer_callback, 100, ctx.cache, `results:${ctx.sn}`, rep, retry + 1, retry);
}
exports.waiting = waiting;
function wait_for_response(cache, reply, rep, retry = 7) {
    setTimeout(timer_callback, 500, cache, reply, rep, retry + 1, retry);
}
exports.wait_for_response = wait_for_response;
function set_for_response(cache, key, value, timeout = 30) {
    return new Promise((resolve, reject) => {
        msgpack_encode_async(value).then(buf => {
            cache.setex(key, timeout, buf, (e, _) => {
                if (e) {
                    reject(e);
                }
                else {
                    resolve();
                }
            });
        }).catch(e => {
            reject(e);
        });
    });
}
exports.set_for_response = set_for_response;
function async_timer_callback(cache, reply, resolve, reject, retry, countdown) {
    cache.get(reply, (err, result) => {
        if (result) {
            msgpack_decode_async(result).then(obj => {
                resolve(obj);
            }).catch((e) => {
                reject(e);
            });
        }
        else if (countdown === 0) {
            const e = new Error();
            e.name = "504";
            e.message = "Request Timeout";
            reject(e);
        }
        else {
            setTimeout(timer_callback, fib(retry - countdown) * 1000, cache, reply, resolve, reject, retry, countdown - 1);
        }
    });
}
function waitingAsync(ctx, retry = 7) {
    return new Promise((resolve, reject) => {
        setTimeout(async_timer_callback, 100, ctx.cache, `results:${ctx.sn}`, resolve, reject, retry + 1, retry);
    });
}
exports.waitingAsync = waitingAsync;
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
                if (data["payload"][0] === 0x78 && data["payload"][1] === 0x9c) {
                    zlib.inflate(data["payload"], (e, newbuf) => {
                        if (e) {
                            req.close();
                            reject(e);
                        }
                        else {
                            req.close();
                            resolve(msgpack.decode(newbuf));
                        }
                    });
                }
                else {
                    req.close();
                    resolve(msgpack.decode(data["payload"]));
                }
            }
            else {
                req.close();
                reject(new Error("Invalid calling sequence number"));
            }
        });
        req.send(msgpack.encode({ sn, pkt: params }));
    });
    return p;
}
exports.rpc = rpc;
function msgpack_encode_async(obj) {
    return new Promise((resolve, reject) => {
        const buf = msgpack.encode(obj);
        if (buf.length > 1024) {
            return zlib.deflate(buf, (e, newbuf) => {
                if (e) {
                    reject(e);
                }
                else {
                    resolve(newbuf);
                }
            });
        }
        else {
            resolve(buf);
        }
    });
}
exports.msgpack_encode_async = msgpack_encode_async;
function msgpack_encode(obj, cb) {
    const buf = msgpack.encode(obj);
    if (buf.length > 1024) {
        zlib.deflate(buf, (e, newbuf) => {
            cb(e, newbuf);
        });
    }
    else {
        cb(null, buf);
    }
}
exports.msgpack_encode = msgpack_encode;
function msgpack_decode_async(buf) {
    return new Promise((resolve, reject) => {
        if (buf[0] === 0x78 && buf[1] === 0x9c) {
            zlib.inflate(buf, (e, newbuf) => {
                if (e) {
                    reject(e);
                }
                else {
                    const result = msgpack.decode(newbuf);
                    resolve(result);
                }
            });
        }
        else {
            const result = msgpack.decode(buf);
            resolve(result);
        }
    });
}
exports.msgpack_decode_async = msgpack_decode_async;
function msgpack_decode(buf, cb) {
    if (buf[0] === 0x78 && buf[1] === 0x9c) {
        zlib.inflate(buf, (e, newbuf) => {
            if (e) {
                cb(e, null);
            }
            else {
                try {
                    const result = msgpack.decode(buf);
                    cb(null, result);
                }
                catch (e) {
                    cb(e, null);
                }
            }
        });
    }
    else {
        try {
            const result = msgpack.decode(buf);
            cb(null, result);
        }
        catch (e) {
            cb(e, null);
        }
    }
}
exports.msgpack_decode = msgpack_decode;
