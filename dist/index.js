"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const msgpack = require("msgpack-lite");
const nanomsg_1 = require("nanomsg");
const fs = require("fs");
const bluebird = require("bluebird");
const zlib = require("zlib");
const pg_1 = require("pg");
const redis_1 = require("redis");
const hive_disque_1 = require("hive-disque");
const http = require("http");
;
class QueueProvider {
    constructor(addresses) {
        this.addresses = addresses;
    }
    instance() {
        if (this.disque === undefined) {
            this.disque = new hive_disque_1.Disq({ nodes: this.addresses });
        }
        return this.disque;
    }
    error(e) {
        if (e.message.indexOf("ECONNREFUSED") !== -1) {
            this.disque = undefined;
        }
    }
}
exports.QueueProvider = QueueProvider;
function get_response_addresses(addr) {
    const lastnumber = parseInt(addr[addr.length - 1]);
    const prefix = addr.substr(0, addr.length - 1);
    const addresses = [];
    const limit = parseInt(process.env["MAX_GATEWAY"] || 3);
    for (let i = 0; i < limit; i++) {
        addresses.push(prefix + (lastnumber + i).toString());
    }
    return addresses;
}
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
    init(modname, serveraddr, queueaddr, cache, loginfo, logerror, queue_provider) {
        function handle_function_exception(ctx, sock, fun, sn, e) {
            logerror(e);
            ctx.report(0, e);
            const regexp = /[0-9]{3}/;
            const payload = msgpack.encode((e.name && regexp.test(e.name)) ? { code: e.name, msg: e.message } : { code: 500, msg: e.stack + " function: " + fun });
            msgpack_encode({ sn, payload }, (e, pkt) => {
                if (e) {
                    logerror(e);
                }
                else {
                    sock.send(pkt);
                }
            });
        }
        ;
        this.queueaddr = queueaddr;
        if (this.queueaddr) {
            this.pub = nanomsg_1.socket("pub");
            this.pub.bind(this.queueaddr);
        }
        this.loginfo = loginfo;
        this.logerror = logerror;
        this.queue_provider = queue_provider;
        for (const sock of get_response_addresses(serveraddr).map(x => { loginfo(x); const pair = nanomsg_1.socket("pair"); pair.bind(x); return pair; })) {
            sock.on("data", ((buf) => {
                const data = msgpack.decode(buf);
                const pkt = data.pkt;
                const sn = data.sn;
                const ctx = Object.assign({}, pkt.ctx, { modname,
                    cache, publish: undefined, push: undefined, report: undefined, sn });
                const fun = pkt.fun;
                const args = pkt.args;
                if (this.permissions.has(fun) && this.permissions.get(fun).get(ctx.domain)) {
                    const [asynced, impl] = this.functions.get(fun);
                    ctx.publish = this.pub ? (pkt) => this.pub.send(msgpack.encode(Object.assign({}, pkt, { sn, domain: ctx.domain, uid: ctx.uid }))) : (pkt) => { logerror("Publish channel not exists"); };
                    ctx.push = (queuename, data, qsn) => {
                        const event = {
                            sn: qsn || sn,
                            data,
                            domain: ctx.domain,
                            uid: ctx.uid,
                        };
                        if (this.queue_provider) {
                            msgpack_encode(event, (e, pkt) => {
                                if (e) {
                                    logerror(e);
                                }
                                else {
                                    this.queue_provider.instance().addjob(queuename, pkt, { retry: 0 }, (e) => {
                                        if (e) {
                                            logerror(e);
                                            this.queue_provider.error(e);
                                        }
                                    });
                                }
                            });
                        }
                    };
                    ctx.report = this.queue_provider ? (level, error) => {
                        const payload = {
                            module: modname,
                            function: fun,
                            level,
                            error,
                            args,
                        };
                        const pkt = msgpack.encode(payload);
                        this.queue_provider.instance().addjob("hive-errors", pkt, { retry: 0 }, (e) => {
                            if (e) {
                                this.logerror(e);
                                this.queue_provider.error(e);
                            }
                        });
                    } : (level, error) => {
                    };
                    if (!asynced) {
                        const func = impl;
                        try {
                            args ? func(ctx, (result) => {
                                server_msgpack(sn, result, (buf) => { sock.send(buf); });
                            }, ...args) : func(ctx, (result) => {
                                server_msgpack(sn, result, (buf) => { sock.send(buf); });
                            });
                        }
                        catch (e) {
                            handle_function_exception(ctx, sock, fun, sn, e);
                        }
                    }
                    else {
                        const func = impl;
                        const result = args ? func(ctx, ...args) : func(ctx);
                        result.then(result => {
                            server_msgpack(sn, result, (buf) => { sock.send(buf); });
                        }).catch(e => {
                            handle_function_exception(ctx, sock, fun, sn, e);
                        });
                    }
                }
                else {
                    const payload = msgpack.encode({ code: 403, msg: "Forbidden" });
                    sock.send(msgpack.encode({ sn, payload }));
                }
            }).bind(this));
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
function report_processor_error(ctx, fun, level, e, args) {
    ctx.logerror(e);
    const payload = {
        module: ctx.modname,
        function: fun,
        level: 0,
        error: e,
        args,
    };
    const pkt = msgpack.encode(payload);
    if (ctx.queue_provider) {
        ctx.queue_provider.instance().addjob("hive-errors", pkt, { retry: 0 }, (e) => {
            if (e) {
                ctx.logerror(e);
                ctx.queue_provider.error(e);
            }
        });
    }
}
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
    init(modname, queueaddr, pool, cache, loginfo, logerror, queue_provider) {
        this.modname = modname;
        this.queueaddr = queueaddr;
        this.sock = nanomsg_1.socket("sub");
        this.sock.connect(this.queueaddr);
        this.queue_provider = queue_provider;
        if (this.subqueueaddr) {
            this.pub = nanomsg_1.socket("pub");
            this.pub.bind(this.subqueueaddr);
            for (const subprocessor of this.subprocessors) {
                subprocessor.init(modname, this.subqueueaddr, pool, cache, loginfo, logerror, queue_provider);
            }
        }
        this.sock.on("data", ((buf) => {
            const pkt = msgpack.decode(buf);
            if (this.functions.has(pkt.cmd)) {
                const [asynced, impl] = this.functions.get(pkt.cmd);
                pool.connect((err, db, done) => {
                    if (err) {
                        const ctx = {
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
                    }
                    else {
                        const ctx = {
                            modname,
                            db,
                            cache,
                            domain: pkt.domain,
                            uid: pkt.uid,
                            sn: pkt.sn,
                            queue_provider,
                            publish: (pkt) => this.pub ? this.pub.send(msgpack.encode(pkt)) : undefined,
                            push: (queuename, data, qsn) => {
                                const event = {
                                    sn: qsn || pkt.sn,
                                    data,
                                    domain: ctx.domain,
                                    uid: ctx.uid,
                                };
                                if (this.queue_provider) {
                                    msgpack_encode(event, (e, pkt) => {
                                        if (e) {
                                            logerror(e);
                                        }
                                        else {
                                            this.queue_provider.instance().addjob(queuename, pkt, { retry: 0 }, (e) => {
                                                if (e) {
                                                    logerror(e);
                                                    this.queue_provider.error(e);
                                                }
                                            });
                                        }
                                    });
                                }
                            },
                            report: this.queue_provider ? (level, error) => {
                                const payload = {
                                    module: modname,
                                    function: pkt.cmd,
                                    level,
                                    error,
                                };
                                const epkt = msgpack.encode(payload);
                                this.queue_provider.instance().addjob("hive-errors", epkt, { retry: 0 }, (e) => {
                                    if (e) {
                                        logerror(e);
                                        this.queue_provider.error(e);
                                    }
                                });
                            } : (level, error) => {
                                logerror(error);
                            },
                            logerror,
                        };
                        if (!asynced) {
                            const func = impl;
                            try {
                                pkt.args ? func(ctx, ...pkt.args) : func(ctx);
                            }
                            catch (e) {
                                report_processor_error(ctx, pkt.cmd, 0, e, pkt.args);
                            }
                            finally {
                                done();
                            }
                        }
                        else {
                            const func = impl;
                            const r = pkt.args ? func(ctx, ...pkt.args) : func(ctx);
                            r.then(result => {
                                done();
                                if (result !== undefined) {
                                    msgpack_encode(result, (e, buf) => {
                                        if (e) {
                                            report_processor_error(ctx, "processor/asyncfunc/msgpack_encode", 0, e);
                                        }
                                        else {
                                            cache.setex(`results:${pkt.sn}`, 600, buf, (e, _) => {
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
                                msgpack_encode({ code: 500, msg: e.message }, (e, buf) => {
                                    if (e) {
                                        report_processor_error(ctx, "processor/asyncfunc-catch/msgpack_encode", 0, e);
                                    }
                                    else {
                                        cache.setex(`results:${pkt.sn}`, 600, buf, (e, _) => {
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
            }
            else {
                loginfo(pkt.cmd + " not found!");
            }
        }).bind(this));
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
function report_timer_error(ctx, fun, level, e, callback) {
    ctx.logerror(e);
    const payload = {
        module: ctx.modname,
        function: fun,
        level: 0,
        error: e,
    };
    const pkt = msgpack.encode(payload);
    ctx.queue_provider.instance().addjob("hive-errors", pkt, { retry: 0 }, (e) => {
        if (e) {
            ctx.logerror(e);
            ctx.queue_provider.error(e);
        }
        callback();
    });
}
function business_event_loop(ctx) {
    const options = {
        timeout: 10,
        count: 1,
    };
    ctx.queue_provider.instance().getjob(ctx.queuename, options, (e, jobs) => {
        if (e) {
            ctx.logerror(e);
            ctx.queue_provider.error(e);
            report_timer_error(ctx, "business_event_loop/getjob.error", 0, e, () => { setTimeout(business_event_loop, 1000, ctx); });
            return;
        }
        if (jobs.length > 0) {
            const job = jobs[0];
            const body = job.body;
            msgpack_decode(body, (e, pkt) => {
                if (e) {
                    report_timer_error(ctx, "business_event_loop/getjob/msgpack_decode", 0, e, () => { setTimeout(business_event_loop, 1000, ctx); });
                }
                else {
                    ctx.pool.connect((err, db, done) => {
                        if (err) {
                            report_timer_error(ctx, "business_event_loop/getjob/msgpack_decode/connect", 0, err, () => { setTimeout(business_event_loop, 1000, ctx); });
                        }
                        else {
                            ctx.db = db;
                            ctx.domain = pkt.domain;
                            ctx.uid = pkt.uid;
                            ctx.handler(ctx, pkt.data).then(result => {
                                done();
                                if (result !== undefined) {
                                    msgpack_encode(result, (e, buf) => {
                                        if (e) {
                                            report_timer_error(ctx, "business_event_loop/getjob/msgpack_decode/connect/handle.then/msgpack_encode", 0, e, () => { setTimeout(business_event_loop, 1000, ctx); });
                                        }
                                        else {
                                            ctx.cache.setex(`results:${pkt.sn}`, 600, buf, (e, _) => {
                                                if (e) {
                                                    report_timer_error(ctx, "business_event_loop/getjob/msgpack_decode/connect/handle.then/msgpack_encode/setex", 0, e, () => { setTimeout(business_event_loop, 1000, ctx); });
                                                }
                                                else {
                                                    setTimeout(business_event_loop, 0, ctx);
                                                }
                                            });
                                        }
                                    });
                                }
                                else {
                                    msgpack_encode({ code: 500, msg: `${ctx.modname} 的 BusinessEventHandlerFunction 没有返回结果` }, (e, buf) => {
                                        if (e) {
                                            ctx.logerror(e);
                                            report_timer_error(ctx, "business_event_loop/getjob/msgpack_decode/connect/handle.then/msgpack_encode", 0, e, () => { setTimeout(business_event_loop, 1000, ctx); });
                                        }
                                        else {
                                            ctx.cache.setex(`results:${pkt.sn}`, 600, buf, (e, _) => {
                                                if (e) {
                                                    report_timer_error(ctx, "business_event_loop/getjob/msgpack_decode/connect/handle.then/msgpack_encode/setex", 0, e, () => { setTimeout(business_event_loop, 1000, ctx); });
                                                }
                                                else {
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
                                    msgpack_encode({ code: 500, msg: e.message }, (e, buf) => {
                                        if (e) {
                                            report_timer_error(ctx, "business_event_loop/getjob/msgpack_decode/connect/handle.catch/msgpack_encode", 0, e, () => { setTimeout(business_event_loop, 1000, ctx); });
                                        }
                                        else {
                                            ctx.cache.setex(`results:${pkt.sn}`, 600, buf, (e, _) => {
                                                if (e) {
                                                    report_timer_error(ctx, "business_event_loop/getjob/msgpack_decode/connect/handle.catch/msgpack_encode/setex", 0, e, () => { setTimeout(business_event_loop, 1000, ctx); });
                                                }
                                                else {
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
        }
        else {
            setTimeout(business_event_loop, 1000, ctx);
        }
    });
}
class BusinessEventListener {
    constructor(queuename) {
        this.queuename = queuename;
    }
    init(modname, pool, cache, loginfo, logerror, queue_provider) {
        const ctx = {
            pool,
            cache,
            queue_provider,
            queuename: this.queuename,
            handler: this.handler,
            report: (level, error) => {
                const payload = {
                    module: modname,
                    function: this.queuename,
                    level,
                    error,
                };
                const pkt = msgpack.encode(payload);
                queue_provider.instance().addjob("hive-errors", pkt, { retry: 0 }, (e) => {
                    if (e) {
                        logerror(e);
                        queue_provider.error(e);
                    }
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
        if (this.config.queueaddr) {
            const path = this.config.queueaddr.substring(this.config.queueaddr.indexOf("///") + 2, this.config.queueaddr.length);
            if (fs.existsSync(path)) {
                fs.unlinkSync(path);
            }
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
            max: 2 * (this.processors.length + this.listeners.length),
            idleTimeoutMillis: 30000,
        };
        const pool = new pg_1.Pool(dbconfig);
        if (this.config.queuehost) {
            const port = this.config.queueport ? this.config.queueport : 7711;
            const queue = new hive_disque_1.Disq({ nodes: [`${this.config.queuehost}:${port}`] });
            this.server.init(this.config.modname, this.config.serveraddr, this.config.queueaddr, cacheAsync, this.config.loginfo || console.log, this.config.logerror || console.log, new QueueProvider([`${this.config.queuehost}:${port}`]));
            for (const processor of this.processors) {
                processor.init(this.config.modname, this.config.queueaddr, pool, cacheAsync, this.config.loginfo || console.log, this.config.logerror || console.log, new QueueProvider([`${this.config.queuehost}:${port}`]));
            }
            for (const listener of this.listeners) {
                listener.init(this.config.modname, pool, cacheAsync, this.config.loginfo || console.log, this.config.logerror || console.log, new QueueProvider([`${this.config.queuehost}:${port}`]));
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
                    msg: e.message,
                });
            });
        }
        else if (countdown === 0) {
            rep({
                code: 504,
                msg: "Request Timeout",
            });
        }
        else {
            setTimeout(timer_callback, fib(retry - countdown) * 1000, cache, reply, rep, retry, countdown - 1);
        }
    });
}
function waiting(ctx, rep, sn = ctx.sn, retry = 7) {
    setTimeout(timer_callback, 100, ctx.cache, `results:${sn}`, rep, retry + 1, retry);
}
exports.waiting = waiting;
function wait_for_response(cache, reply, rep, retry = 7) {
    setTimeout(timer_callback, 100, cache, reply, rep, retry + 1, retry);
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
            setTimeout(async_timer_callback, fib(retry - countdown) * 1000, cache, reply, resolve, reject, retry, countdown - 1);
        }
    });
}
function waitingAsync(ctx, sn = ctx.sn, retry = 7) {
    return new Promise((resolve, reject) => {
        setTimeout(async_timer_callback, 100, ctx.cache, `results:${sn}`, resolve, reject, retry + 1, retry);
    });
}
exports.waitingAsync = waitingAsync;
function rpc(domain, addr, uid, cb, fun, ...args) {
    const host = process.env["RPC-HOST"] || "127.0.0.1";
    const port = process.env["GATEWAY-" + (domain.toUpperCase()) + "-PORT"] || 8000;
    const path = process.env["RPC-PATH"] || "/";
    const openid = uid;
    const mod = Object.keys(process.env).filter(x => process.env[x] === addr);
    const data = msgpack.encode({
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
        const chunks = [];
        rep.on("data", (chunk) => {
            const buf = chunk;
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
    req.write(data);
    req.end();
}
exports.rpc = rpc;
function rpcAsync(domain, addr, uid, fun, ...args) {
    const host = process.env["RPC-HOST"] || "127.0.0.1";
    const port = process.env["GATEWAY-" + (domain.toUpperCase()) + "-PORT"] || 8000;
    const path = process.env["RPC-PATH"] || "/";
    const openid = uid;
    const mods = Object.keys(process.env).filter(x => process.env[x] === addr);
    if (mods === null || mods.length === 0) {
        return Promise.resolve({ code: 404, msg: `Module with address(${addr}) not found!` });
    }
    const data = msgpack.encode({
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
    const p = new Promise((resolve, reject) => {
        const req = http.request(options, (rep) => {
            const chunks = [];
            rep.on("data", (chunk) => {
                const buf = chunk;
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
        req.write(data);
        req.end();
    });
    return p;
}
exports.rpcAsync = rpcAsync;
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
