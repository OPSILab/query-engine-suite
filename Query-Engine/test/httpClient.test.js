// utils/httpClient.js against axios: the same requests to a local server with both, compared on what the services read
// (status, data, headers, error code and message, config). axios is a devDependency only for these tests: when they
// are removed, the expected values seen here can replace it.
const { test, describe, before, after } = require("node:test")
const assert = require("node:assert/strict")
const http = require("node:http")
const zlib = require("node:zlib")
const axios = require("axios")
const httpClient = require("../utils/httpClient")

let server, other, base, otherBase, closedPort
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms))

function handler(req, res) {
    const url = new URL(req.url, "http://x")
    const chunks = []
    req.on("data", chunk => chunks.push(chunk))
    req.on("end", async () => {
        const body = Buffer.concat(chunks).toString("utf8")
        const json = (status, value, headers = {}) => { res.writeHead(status, { "Content-Type": "application/json", ...headers }); res.end(JSON.stringify(value)) }
        const p = url.pathname
        if (p == "/echo") {
            const { "user-agent": _, host, connection, ...headers } = req.headers
            return json(200, { method: req.method, url: req.url, headers, body })
        }
        if (p == "/json") return json(200, { a: 1, list: [1, "x"] }, { "X-Custom": "yes", "Set-Cookie": ["a=1", "b=2"] })
        if (p == "/text") { res.writeHead(200, { "Content-Type": "text/plain" }); return res.end("hello") }
        if (p == "/notjson") { res.writeHead(200, { "Content-Type": "application/json" }); return res.end("{bad") }
        if (p == "/number") { res.writeHead(200, { "Content-Type": "application/json" }); return res.end("42") }
        if (p == "/bom") { res.writeHead(200, { "Content-Type": "application/json" }); return res.end("﻿{\"b\":true}") }
        if (p == "/empty") { res.writeHead(204); return res.end() }
        if (p == "/empty200") { res.writeHead(200, { "Content-Type": "application/json" }); return res.end() }
        if (p.startsWith("/status/")) return json(Number(p.split("/")[2]), { error: "status " + p.split("/")[2] })
        if (p == "/text500") { res.writeHead(500, { "Content-Type": "text/plain" }); return res.end("boom") }
        if (p == "/slow") { await sleep(400); return json(200, { slow: true }) }
        if (p == "/slowbody") { res.writeHead(200, { "Content-Type": "application/json" }); res.write("{\"a\":"); await sleep(400); return res.end("1}") }
        if (p == "/reset") return req.socket.destroy()
        if (p == "/resetbody") { res.writeHead(200, { "Content-Type": "application/json", "Content-Length": "100" }); res.write("{\"a\":"); await sleep(50); return req.socket.destroy() }
        const payload = Buffer.from(JSON.stringify({ compressed: true, text: "àèìòù ".repeat(50) }))
        if (p == "/gzip") { res.writeHead(200, { "Content-Type": "application/json", "Content-Encoding": "gzip" }); return res.end(zlib.gzipSync(payload)) }
        if (p == "/deflate") { res.writeHead(200, { "Content-Type": "application/json", "Content-Encoding": "deflate" }); return res.end(zlib.deflateSync(payload)) }
        if (p == "/deflateraw") { res.writeHead(200, { "Content-Type": "application/json", "Content-Encoding": "deflate" }); return res.end(zlib.deflateRawSync(payload)) }
        if (p == "/br") { res.writeHead(200, { "Content-Type": "application/json", "Content-Encoding": "br" }); return res.end(zlib.brotliCompressSync(payload)) }
        if (p == "/gzipempty") { res.writeHead(200, { "Content-Type": "application/json", "Content-Encoding": "gzip" }); return res.end() }
        if (p == "/redirect/same") { res.writeHead(302, { Location: "/echo?from=same" }); return res.end() }
        if (p == "/redirect/other") { res.writeHead(302, { Location: otherBase + "/echo?from=other" }); return res.end() }
        if (p == "/redirect/301") { res.writeHead(301, { Location: "/echo" }); return res.end() }
        if (p == "/redirect/303") { res.writeHead(303, { Location: "/echo" }); return res.end() }
        if (p == "/redirect/307") { res.writeHead(307, { Location: "/echo" }); return res.end() }
        if (p == "/redirect/loop") { res.writeHead(302, { Location: "/redirect/loop" }); return res.end() }
        json(404, { error: "no route " + p })
    })
}

before(async () => {
    server = http.createServer(handler)
    other = http.createServer(handler)
    await new Promise(resolve => server.listen(0, "127.0.0.1", resolve))
    await new Promise(resolve => other.listen(0, "127.0.0.1", resolve))
    base = `http://127.0.0.1:${server.address().port}`
    otherBase = `http://localhost:${other.address().port}` // another host name: no Authorization after the redirect
    const closed = http.createServer()
    await new Promise(resolve => closed.listen(0, "127.0.0.1", resolve))
    closedPort = closed.address().port
    await new Promise(resolve => closed.close(resolve))
})
after(() => { server?.close(); other?.close(); server?.closeAllConnections?.(); other?.closeAllConnections?.() })

// plain objects; the User-Agent is not the same on purpose ("axios/x" / "query-engine-suite")
function plain(headers) {
    if (!headers)
        return headers
    const result = { ...(typeof headers.toJSON == "function" ? headers.toJSON() : headers) }
    for (const name of Object.keys(result))
        if (name.toLowerCase() == "user-agent")
            delete result[name]
    return result
}
const dataOf = data => Buffer.isBuffer(data) ? { buffer: data.toString("base64") } : data

// what the services read of an answer / an error
function shape(result) {
    const response = result?.response && !result.status ? result.response : result?.status ? result : undefined
    const out = {}
    if (response) {
        const { "user-agent": _, date, ...headers } = plain(response.headers) || {}
        delete headers["keep-alive"]
        delete headers.connection
        Object.assign(out, { status: response.status, statusText: response.statusText, data: dataOf(response.data), headers })
    }
    if (result instanceof Error)
        Object.assign(out, { error: { message: result.message, code: result.code, status: result.status, hasResponse: !!result.response } })
    const config = result?.config
    if (config)
        out.config = { method: config.method, url: config.url, params: config.params, data: dataOf(config.data), headers: plain(config.headers) }
    return out
}

const run = async (client, call) => {
    try {
        return shape(await call(client))
    }
    catch (error) {
        return shape(error)
    }
}

async function same(call) {
    const expected = await run(axios, call)
    const actual = await run(httpClient, call)
    assert.deepEqual(actual, expected)
    return actual
}

describe("as axios: answers", () => {
    for (const path of ["/json", "/text", "/notjson", "/number", "/bom", "/empty", "/empty200", "/gzip", "/deflate", "/deflateraw", "/br", "/gzipempty"])
        test(`GET ${path}`, () => same(c => c.get(base + path)))

    test("responseType text and arraybuffer", async () => {
        await same(c => c.get(base + "/json", { responseType: "text" }))
        await same(c => c.get(base + "/gzip", { responseType: "arraybuffer" }))
    })

    test("HEAD, DELETE, OPTIONS", async () => {
        await same(c => c.head(base + "/json"))
        await same(c => c.delete(base + "/echo", { headers: { "X-Del": "1" } }))
        await same(c => c.options(base + "/echo"))
    })
})

describe("as axios: requests", () => {
    test("params: arrays, repeated (indexes: null) or numbered, nested objects, dates, special characters", async () => {
        const params = { a: [1, 2], s: "a b:$,'!()*~àé&=?/#", d: new Date("2026-01-02T03:04:05Z"), n: null, u: undefined, b: true, o: { x: 1, y: [3, 4] }, "k[]": ["v"], e: "" }
        await same(c => c.get(base + "/echo", { params }))
        await same(c => c.get(base + "/echo", { params, paramsSerializer: { indexes: null } }))
        await same(c => c.get(base + "/echo", { params, paramsSerializer: { indexes: true } }))
        await same(c => c.get(base + "/echo?x=1#frag", { params: { y: 2 } }))
        await same(c => c.get(base + "/echo?x=1", { params: new URLSearchParams({ z: "a b" }) }))
        await same(c => c.get(base + "/echo/àè space?q=è", {}))
        const fhir = { _count: 1000, _lastUpdated: ["ge2026-01-01T00:00:00.000Z", "lt2026-01-02T00:00:00.000Z"] }
        await same(c => c.get(base + "/echo", { params: fhir, paramsSerializer: { indexes: null } }))
        for (const config of [{ url: base + "/echo", params: fhir, paramsSerializer: { indexes: null } }, { url: base + "/echo?a=1", params: { b: [1, 2] } }, { url: "/rel", baseURL: base + "/api/" }])
            assert.equal(httpClient.getUri(config), axios.getUri(config))
    })

    test("bodies: object (JSON), array, URLSearchParams, string, Buffer, number, nothing", async () => {
        await same(c => c.post(base + "/echo", { id: 7, nested: { a: [1] } }, { headers: { Authorization: "Bearer t", "X-Num": 5 } }))
        await same(c => c.post(base + "/echo", [1, 2]))
        await same(c => c.post(base + "/echo", new URLSearchParams({ grant_type: "client_credentials", scope: "a b" })))
        await same(c => c.post(base + "/echo", "plain text"))
        await same(c => c.post(base + "/echo", "{\"j\":1}", { headers: { "Content-Type": "application/json" } }))
        await same(c => c.put(base + "/echo", Buffer.from("bytes")))
        await same(c => c.patch(base + "/echo", { p: 1 }))
        await same(c => c.post(base + "/echo"))
        await same(c => c.post(base + "/echo", { a: 1, b: [1, 2] }, { headers: { "Content-Type": "application/x-www-form-urlencoded" } }))
    })

    test("headers: case kept, Accept and Content-Type not doubled, undefined / null left out", async () => {
        await same(c => c.get(base + "/echo", { headers: { accept: "text/plain", "x-a": undefined, "X-B": null, "X-C": "" } }))
        await same(c => c.post(base + "/echo", { a: 1 }, { headers: { "content-type": "application/json; charset=utf-8" } }))
        await same(c => c.request({ method: "POST", url: base + "/echo", data: { a: 1 } }))
        await same(c => c.request(base + "/echo", { method: "put", data: "x" }))
    })
})

describe("as axios: errors", () => {
    test("4xx / 5xx: error.response, error.status, codes", async () => {
        for (const path of ["/status/400", "/status/401", "/status/404", "/status/500", "/status/503", "/text500"])
            await same(c => c.get(base + path))
        await same(c => c.get(base + "/status/404", { validateStatus: status => status < 500 }))
        await same(c => c.get(base + "/status/200", { validateStatus: () => false }))
    })

    test("no answer: connection refused, reset, reset during the body, unknown host", async () => {
        await same(c => c.get(`http://127.0.0.1:${closedPort}/x`))
        await same(c => c.get(base + "/reset"))
        await same(c => c.get(base + "/resetbody"))
        const a = await run(axios, c => c.get("http://no-such-host.invalid/x"))
        const b = await run(httpClient, c => c.get("http://no-such-host.invalid/x"))
        assert.deepEqual(b.error.code, a.error.code)
    })

    test("timeout before the answer and during the body", async () => {
        await same(c => c.get(base + "/slow", { timeout: 100 }))
        await same(c => c.get(base + "/slowbody", { timeout: 100 }))
        await same(c => c.get(base + "/slow", { timeout: 100, timeoutErrorMessage: "too slow" }))
        await same(c => c.get(base + "/slow", { timeout: 1000 }))
    })

    test("invalid urls and protocols", async () => {
        const a = await run(axios, c => c.get("ftp://127.0.0.1/x"))
        const b = await run(httpClient, c => c.get("ftp://127.0.0.1/x"))
        assert.deepEqual([b.error.code, b.error.message], [a.error.code, a.error.message])
        const c1 = await run(axios, c => c.get("/relative"))
        const c2 = await run(httpClient, c => c.get("/relative"))
        assert.equal(c2.error.code, c1.error.code)
    })
})

describe("as axios: redirects", () => {
    test("same host, other host (no Authorization), 301 / 303 after POST, 307 keeps the body, too many", async () => {
        const auth = { headers: { Authorization: "Bearer secret", Cookie: "c=1", "X-Keep": "1" } }
        await same(c => c.get(base + "/redirect/same", auth))
        const otherHost = await same(c => c.get(base + "/redirect/other", auth))
        assert.equal(otherHost.data.headers.authorization, undefined)
        assert.equal(otherHost.data.headers["x-keep"], "1")
        await same(c => c.post(base + "/redirect/301", { a: 1 }))
        await same(c => c.put(base + "/redirect/303", { a: 1 }))
        await same(c => c.post(base + "/redirect/307", { a: 1 }))
        await same(c => c.get(base + "/redirect/same", { maxRedirects: 0 }))
        const a = await run(axios, c => c.get(base + "/redirect/loop"))
        const b = await run(httpClient, c => c.get(base + "/redirect/loop"))
        assert.deepEqual([b.error.code, b.error.message], [a.error.code, a.error.message])
    })
})

describe("interceptors", () => {
    test("as axios: request ones last-added first, response ones in order, errors through onRejected, eject", async () => {
        const trace = async client => {
            const seen = []
            const ids = [
                client.interceptors.request.use(config => { seen.push("req1"); config.headers["X-Req1"] = "1"; return config }),
                client.interceptors.request.use(config => { seen.push("req2"); config.mark = 7; return config }),
                client.interceptors.response.use(response => { seen.push("res1:" + response.config.mark); return response }, error => { seen.push("err1:" + error.config.mark); return Promise.reject(error) }),
                client.interceptors.response.use(response => { seen.push("res2"); response.extra = true; return response }, error => { seen.push("err2"); return Promise.reject(error) }),
            ]
            const ok = await client.get(base + "/echo")
            await client.get(base + "/status/404").catch(() => { })
            client.interceptors.request.eject(ids[0])
            client.interceptors.request.eject(ids[1])
            client.interceptors.response.eject(ids[2])
            client.interceptors.response.eject(ids[3])
            await client.get(base + "/echo")
            return { seen, extra: ok.extra, header: ok.data.headers["x-req1"] }
        }
        assert.deepEqual(await trace(httpClient), await trace(axios))
    })
})

describe("not in axios' tests above", () => {
    test("responseType stream: the body decoded; isHttpError", async () => {
        const response = await httpClient.get(base + "/gzip", { responseType: "stream" })
        const chunks = []
        for await (const chunk of response.data)
            chunks.push(chunk)
        assert.equal(JSON.parse(Buffer.concat(chunks).toString()).compressed, true)
        const error = await httpClient.get(base + "/status/404").catch(e => e)
        assert.equal(httpClient.isHttpError(error), true)
        assert.deepEqual([error.name, error.request.method, error.request.url], ["HttpError", "get", base + "/status/404"])
    })

    test("a test can replace a method (httpClient.get = ...) and the code using the module sees it", async () => {
        const real = httpClient.get
        httpClient.get = async () => ({ data: "fake" })
        try {
            assert.equal((await require("../utils/httpClient").get("x")).data, "fake")
        }
        finally {
            httpClient.get = real
        }
    })
})

describe("proxy from the environment", () => {
    test("HTTP_PROXY: the request goes through the proxy (absolute url); NO_PROXY: direct; proxy: false: direct", async () => {
        const seen = []
        const proxy = http.createServer((req, res) => { seen.push(req.url); res.writeHead(200, { "Content-Type": "application/json" }); res.end("{\"proxied\":true}") })
        await new Promise(resolve => proxy.listen(0, "127.0.0.1", resolve))
        const names = ["HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy"]
        const saved = Object.fromEntries(names.map(name => [name, process.env[name]]))
        try {
            for (const name of names)
                delete process.env[name]
            process.env.HTTP_PROXY = `http://127.0.0.1:${proxy.address().port}`
            const target = `http://example.test:81/data?x=1`
            assert.deepEqual((await httpClient.get(target)).data, { proxied: true })
            assert.deepEqual(seen, [target])
            process.env.NO_PROXY = "127.0.0.1"
            assert.deepEqual((await httpClient.get(base + "/json")).data.a, 1)
            assert.equal(seen.length, 1)
            delete process.env.NO_PROXY
            assert.equal((await httpClient.get(base + "/json", { proxy: false })).data.a, 1)
            assert.equal(seen.length, 1)
            // https: a CONNECT tunnel through the proxy
            const tunnels = []
            proxy.on("connect", (req, socket) => { tunnels.push(req.url); socket.destroy() })
            process.env.HTTPS_PROXY = process.env.HTTP_PROXY
            await httpClient.get("https://secure.example.test/x", { timeout: 2000 }).catch(() => { })
            assert.deepEqual(tunnels, ["secure.example.test:443"])
        }
        finally {
            for (const name of names)
                if (saved[name] === undefined) delete process.env[name]; else process.env[name] = saved[name]
            proxy.close()
        }
    })
})
