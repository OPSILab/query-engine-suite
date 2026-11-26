// The HTTP client of the Source-Connector and of the Query-Engine, in place of axios: no dependencies (node:http,
// node:https, node:zlib), the same shape as axios for what the services use, so that a call reads as before:
//
//   const httpClient = require("../utils/httpClient")
//   const { data, status, headers } = await httpClient.get(url, { headers, params, timeout: 30000 })
//   await httpClient.post(url, body, { headers })        // an object: sent as JSON; URLSearchParams: as a form
//   httpClient.interceptors.response.use(onResponse, onError)
//
// As axios (1.x):
// - get / delete / head / options (url, config); post / put / patch (url, data, config); request(config) or
//   request(url, config); getUri(config): the url with its params;
// - params: appended to the url as axios does (arrays as p[]=a&p[]=b; paramsSerializer: { indexes: null } gives
//   p=a&p=b, { indexes: true } p[0]=a; nested objects as a[b]=c; dates as ISO text; null / undefined left out);
// - the answer: { data, status, statusText, headers (lowercase names), config, request }; data parsed when it is
//   JSON (else the text); responseType "text" (never parsed), "arraybuffer" (a Buffer), "stream";
// - a status outside 2xx (validateStatus) is an error with error.response and error.status, code ERR_BAD_REQUEST
//   (4xx) or ERR_BAD_RESPONSE (5xx); no answer: the error of Node (code ECONNREFUSED, ECONNRESET, ENOTFOUND...);
//   timeout: "timeout of <n>ms exceeded", code ECONNABORTED (idle socket, as axios); error.config is the request;
// - redirects followed (21 at most, maxRedirects: 0 none); to another host (not a subdomain) or from https to http,
//   without the Authorization / Cookie / Proxy-Authorization headers; 301 / 302 after a POST and 303: a GET;
// - answers gzip / deflate / br decompressed;
// - interceptors.request / .response: use(onFulfilled, onRejected) -> id, eject(id); request ones last-added first;
// - httpAgent / httpsAgent; HTTP_PROXY / HTTPS_PROXY / NO_PROXY (lowercase too) through the agents of Node
//   (proxyEnv, Node >= 22.21 / 24.5): not with your own agent (create it with { proxyEnv: process.env } for that),
//   nor with proxy: false.
// Not as axios: errors are HttpError (name "HttpError", isHttpError; there is no isAxiosError); error.request is
// { method, url } (the url of the last request, after the redirects); the User-Agent is "query-engine-suite";
// no data: URLs, no FormData, no browser.
// The same file is in the Source-Connector and the Query-Engine (utils/httpClient.js).
const nodeHttp = require("node:http")
const nodeHttps = require("node:https")
const zlib = require("node:zlib")
const { Readable } = require("node:stream")

const DEFAULT_ACCEPT = "application/json, text/plain, */*"
const ACCEPT_ENCODING = "gzip, compress, deflate, br"
const USER_AGENT = "query-engine-suite"
const MAX_REDIRECTS = 21
const BODYLESS_METHODS = new Set(["get", "delete", "head", "options"])

class HttpError extends Error {
    constructor(message, code, config, request, response, cause) {
        super(message, cause ? { cause } : undefined)
        this.name = "HttpError"
        this.code = code
        this.config = config
        this.request = request
        this.response = response
        if (response)
            this.status = response.status
        this.isHttpError = true
    }
}

// ---- headers (plain objects; names compared without case)

function headerName(headers, name) {
    const lower = name.toLowerCase()
    return Object.keys(headers).find(key => key.toLowerCase() === lower)
}

function getHeader(headers, name) {
    const key = headerName(headers, name)
    return key === undefined ? undefined : headers[key]
}

// as axios: a header already there keeps the case of its name
function setHeader(headers, name, value, override = true) {
    const key = headerName(headers, name)
    if (key !== undefined && !override)
        return
    headers[key ?? name] = value
}

function removeHeaders(headers, pattern) {
    for (const key of Object.keys(headers))
        if (pattern.test(key))
            delete headers[key]
}

function plainHeaders(headers) {
    const result = {}
    for (const [name, value] of Object.entries(headers || {}))
        if (value !== undefined && value !== null && value !== false)
            setHeader(result, name, Array.isArray(value) ? value : String(value))
    return result
}

// ---- urls and params (as axios: helpers/buildURL.js, helpers/toFormData.js)

function encode(value) {
    return encodeURIComponent(value).replace(/%3A/gi, ":").replace(/%24/g, "$").replace(/%2C/gi, ",").replace(/%20/g, "+")
}

const isPlainObject = value => value !== null && typeof value == "object" && (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null)
const isVisitable = value => isPlainObject(value) || Array.isArray(value)
const removeBrackets = key => typeof key == "string" && key.endsWith("[]") ? key.slice(0, -2) : key

function convertValue(value) {
    if (value === null)
        return ""
    if (value instanceof Date)
        return value.toISOString()
    if (typeof value == "boolean")
        return value.toString()
    if (Buffer.isBuffer(value) || ArrayBuffer.isView(value) || value instanceof ArrayBuffer)
        return Buffer.from(value)
    return value
}

function renderKey(path, key, dots) {
    if (!path)
        return key
    return path.concat(key).map((token, i) => {
        token = removeBrackets(token)
        return !dots && i ? "[" + token + "]" : token
    }).join(dots ? "." : "")
}

// [name, value] pairs of params as axios builds them
function paramPairs(params, { indexes = false, dots = false } = {}) {
    const pairs = []
    const stack = []
    const visit = (value, key, path) => {
        if (value && !path && typeof value == "object") {
            if (typeof key == "string" && key.endsWith("{}")) {
                pairs.push([key, JSON.stringify(value)])
                return false
            }
            if (Array.isArray(value) && !value.some(isVisitable) || typeof key == "string" && key.endsWith("[]") && Array.isArray(value)) {
                const base = removeBrackets(key)
                value.forEach((element, index) => {
                    if (element !== undefined && element !== null)
                        pairs.push([indexes === true ? renderKey([base], index, dots) : indexes === null ? base : base + "[]", convertValue(element)])
                })
                return false
            }
        }
        if (isVisitable(value))
            return true
        pairs.push([renderKey(path, key, dots), convertValue(value)])
        return false
    }
    const build = (value, path) => {
        if (value === undefined)
            return
        if (stack.includes(value))
            throw new Error("Circular reference detected in " + path.join("."))
        stack.push(value)
        for (const [rawKey, element] of Array.isArray(value) ? value.entries() : Object.entries(value)) {
            const key = typeof rawKey == "string" ? rawKey.trim() : rawKey
            if (element !== undefined && element !== null && visit(element, key, path))
                build(element, path ? path.concat(key) : [key])
        }
        stack.pop()
    }
    build(params)
    return pairs
}

function serializeParams(params, serializer) {
    const options = typeof serializer == "function" ? { serialize: serializer } : serializer || {}
    if (typeof options.serialize == "function")
        return options.serialize(params, options)
    if (params instanceof URLSearchParams)
        return params.toString()
    const enc = typeof options.encode == "function" ? options.encode : encode
    return paramPairs(params, options).map(([name, value]) => enc(name) + "=" + enc(value)).join("&")
}

function buildURL(url, params, serializer) {
    if (!params)
        return url
    url = url || ""
    const query = serializeParams(params, serializer)
    if (query) {
        const hash = url.indexOf("#")
        if (hash !== -1)
            url = url.slice(0, hash)
        url += (url.indexOf("?") === -1 ? "?" : "&") + query
    }
    return url
}

const isAbsoluteURL = url => /^([a-z][a-z\d+\-.]*:)?\/\//i.test(url)

function fullPath(config) {
    const { baseURL, url = "" } = config
    if (baseURL && !isAbsoluteURL(url))
        return url ? baseURL.replace(/\/?\/$/, "") + "/" + url.replace(/^\/+/, "") : baseURL
    return url
}

/** The url of a request with its params, as it is sent (axios.getUri). */
function getUri(config = {}) {
    return buildURL(fullPath(config), config.params, config.paramsSerializer)
}

// ---- request body (as axios: defaults/transformRequest, adapters/http.js)

function requestBody(config) {
    let data = config.data
    const headers = config.headers
    if (data === undefined || data === null)
        return { data, body: undefined }
    if (data instanceof Readable)
        return { data, body: data }
    if (Buffer.isBuffer(data))
        return { data, body: data }
    if (data instanceof ArrayBuffer)
        return { data, body: Buffer.from(data) }
    if (ArrayBuffer.isView(data))
        return { data, body: Buffer.from(data.buffer, data.byteOffset, data.byteLength) }
    if (data instanceof URLSearchParams) {
        setHeader(headers, "Content-Type", "application/x-www-form-urlencoded;charset=utf-8", false)
        data = data.toString()
    }
    else {
        const contentType = String(getHeader(headers, "Content-Type") || "")
        if (data !== null && typeof data == "object") {
            if (contentType.includes("application/x-www-form-urlencoded"))
                data = serializeParams(data, { encode: value => encodeURIComponent(value).replace(/%20/g, "+") })
            else {
                setHeader(headers, "Content-Type", "application/json", false)
                data = JSON.stringify(data)
            }
        }
        else if (typeof data == "string" && contentType.includes("application/json")) {
            try {
                JSON.parse(data)
                data = data.trim()
            }
            catch { }
        }
    }
    if (typeof data != "string")
        throw new HttpError("Data after transformation must be a string, an ArrayBuffer, a Buffer, or a Stream", "ERR_BAD_REQUEST", config)
    return { data, body: Buffer.from(data, "utf8") }
}

// ---- proxy from the environment (Node's own support)

const PROXY_VARIABLES = ["HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy"]
let proxyAgents = { key: undefined }

function envProxyAgent(protocol) {
    const env = {}
    for (const name of PROXY_VARIABLES)
        if (process.env[name])
            env[name] = process.env[name]
    if (!env.HTTP_PROXY && !env.HTTPS_PROXY && !env.http_proxy && !env.https_proxy)
        return undefined
    const key = JSON.stringify(env)
    if (proxyAgents.key !== key)
        proxyAgents = { key, http: new nodeHttp.Agent({ keepAlive: true, proxyEnv: env }), https: new nodeHttps.Agent({ keepAlive: true, proxyEnv: env }) }
    return protocol == "https:" ? proxyAgents.https : proxyAgents.http
}

// ---- one request, then the redirects

function isSubdomain(subdomain, domain) {
    const dot = subdomain.length - domain.length - 1
    return dot > 0 && subdomain[dot] === "." && subdomain.endsWith(domain)
}

function sendOnce(url, path, method, headers, body, config) {
    const https = url.protocol == "https:"
    const transport = https ? nodeHttps : nodeHttp
    const agent = (https ? config.httpsAgent : config.httpAgent) || (config.proxy === false ? undefined : envProxyAgent(url.protocol))
    const hostname = url.hostname.startsWith("[") ? url.hostname.slice(1, -1) : url.hostname
    const options = { method: method.toUpperCase(), hostname, port: url.port || undefined, path, headers, agent }
    if (config.auth)
        options.auth = (config.auth.username || "") + ":" + (config.auth.password || "")
    else if (url.username || url.password)
        options.auth = decodeURIComponent(url.username) + ":" + decodeURIComponent(url.password)
    const request = { method: options.method.toLowerCase(), url: url.href }
    return new Promise((resolve, reject) => {
        let settled = false
        // before the answer: the request fails with this error; after it, the body being read stops ("stream has been
        // aborted", as axios)
        const fail = error => {
            if (!settled) {
                settled = true
                reject(error)
            }
        }
        const req = transport.request(options, res => {
            settled = true
            resolve({ res, request })
        })
        req.on("error", error => fail(new HttpError(error.message, error.code, config, request, undefined, error)))
        if (config.timeout > 0)
            req.setTimeout(config.timeout, () => {
                fail(new HttpError(config.timeoutErrorMessage || `timeout of ${config.timeout}ms exceeded`, "ECONNABORTED", config, request))
                req.destroy()
            })
        if (config.signal) {
            const abort = () => { fail(new HttpError("canceled", "ERR_CANCELED", config, request)); req.destroy() }
            if (config.signal.aborted)
                return abort()
            config.signal.addEventListener("abort", abort, { once: true })
        }
        if (body instanceof Readable)
            body.on("error", error => { fail(new HttpError(error.message, error.code, config, request, undefined, error)); req.destroy() }).pipe(req)
        else
            req.end(body)
    })
}

async function sendFollowing(config, url, path, method, headers, body) {
    const maxRedirects = config.maxRedirects === undefined ? MAX_REDIRECTS : config.maxRedirects
    for (let redirects = 0; ; redirects++) {
        const sent = await sendOnce(url, path, method, headers, body, config)
        const { res } = sent
        const location = res.headers.location
        if (!(maxRedirects > 0 && location && res.statusCode >= 300 && res.statusCode < 400))
            return sent
        res.resume()
        if (redirects >= maxRedirects)
            throw new HttpError("Maximum number of redirects exceeded", "ERR_FR_TOO_MANY_REDIRECTS", config, sent.request)
        if ((res.statusCode == 301 || res.statusCode == 302) && method == "post" || res.statusCode == 303 && !/^(get|head)$/.test(method)) {
            method = "get"
            body = undefined
            removeHeaders(headers, /^content-/i)
        }
        removeHeaders(headers, /^host$/i)
        let next
        try {
            next = new URL(location, url)
        }
        catch (error) {
            throw new HttpError(`Redirected request failed: ${error.message}`, "ERR_FR_REDIRECTION_FAILURE", config, sent.request, undefined, error)
        }
        if (next.protocol != "http:" && next.protocol != "https:")
            throw new HttpError(`Unsupported protocol ${next.protocol}`, "ERR_FR_REDIRECTION_FAILURE", config, sent.request)
        if (next.protocol !== url.protocol && next.protocol !== "https:" || next.host !== url.host && !isSubdomain(next.host, url.host))
            removeHeaders(headers, /^(?:(?:proxy-)?authorization|cookie)$/i)
        url = next
        path = next.pathname + next.search
    }
}

// ---- the answer

const SYNC_FLUSH = { finishFlush: zlib.constants.Z_SYNC_FLUSH }
const BROTLI_FLUSH = { finishFlush: zlib.constants.BROTLI_OPERATION_FLUSH }

// The encoding of an answer to decode (Content-Encoding), undefined when there is nothing to decode
function contentEncoding(res, method) {
    const encoding = String(res.headers["content-encoding"] || "").trim().toLowerCase()
    if (method == "head" || res.statusCode == 204 || res.statusCode == 304 || !["gzip", "x-gzip", "compress", "x-compress", "deflate", "br"].includes(encoding))
        return undefined
    delete res.headers["content-encoding"]
    return encoding
}

// The body decoded: gzip, deflate (zlib or raw), br; tolerant of a truncated end, as axios
function decodeBody(buffer, encoding) {
    if (!encoding || !buffer.length)
        return buffer
    if (encoding == "br")
        return zlib.brotliDecompressSync(buffer, BROTLI_FLUSH)
    if (encoding == "deflate" && (buffer[0] & 0x0f) !== 0x08)
        return zlib.inflateRawSync(buffer, SYNC_FLUSH)
    return zlib.unzipSync(buffer, SYNC_FLUSH)
}

// responseType "stream": the body as a stream, decoded
function decodeStream(res, encoding) {
    if (!encoding)
        return res
    return res.pipe(encoding == "br" ? zlib.createBrotliDecompress(BROTLI_FLUSH) : encoding == "deflate" ? zlib.createUnzip(SYNC_FLUSH) : zlib.createUnzip(SYNC_FLUSH))
}

function readAll(res) {
    return new Promise((resolve, reject) => {
        const chunks = []
        res.on("data", chunk => chunks.push(chunk))
        const aborted = () => reject(Object.assign(new Error("stream has been aborted"), { code: "ERR_BAD_RESPONSE" }))
        res.on("end", () => res.complete === false ? aborted() : resolve(Buffer.concat(chunks)))
        res.on("error", aborted)
        res.on("aborted", aborted)
    })
}

function responseData(buffer, responseType) {
    if (responseType == "arraybuffer")
        return buffer
    let text = buffer.toString("utf8")
    if (text.charCodeAt(0) === 0xfeff)
        text = text.slice(1)
    if (text && (!responseType || responseType == "json"))
        try {
            return JSON.parse(text)
        }
        catch { }
    return text
}

const defaultValidateStatus = status => status >= 200 && status < 300

async function dispatch(config) {
    config.headers = plainHeaders(config.headers)
    const { data, body } = requestBody(config)
    config.data = data
    // as axios: a body method without a Content-Type is sent as a form
    if (["post", "put", "patch"].includes(config.method))
        setHeader(config.headers, "Content-Type", "application/x-www-form-urlencoded", false)
    // as axios: these end up in config.headers too (what requestLog saves)
    const headers = config.headers
    setHeader(headers, "User-Agent", USER_AGENT, false)
    setHeader(headers, "Accept-Encoding", ACCEPT_ENCODING, false)
    if (Buffer.isBuffer(body))
        setHeader(headers, "Content-Length", String(body.length), false)
    let parsed
    try {
        parsed = new URL(fullPath(config))
    }
    catch (error) {
        throw new HttpError("Invalid URL", "ERR_INVALID_URL", config, undefined, undefined, error)
    }
    if (parsed.protocol != "http:" && parsed.protocol != "https:")
        throw new HttpError(`Unsupported protocol ${parsed.protocol}`, "ERR_BAD_REQUEST", config)
    const path = buildURL(parsed.pathname + parsed.search, config.params, config.paramsSerializer)
    const { res, request } = await sendFollowing(config, parsed, path, config.method, { ...headers }, body)
    const encoding = contentEncoding(res, config.method)
    const response = { status: res.statusCode, statusText: res.statusMessage, headers: res.headers, config, request }
    if (config.responseType == "stream")
        response.data = decodeStream(res, encoding)
    else {
        let buffer
        try {
            buffer = await readAll(res)
        }
        catch (error) {
            throw error instanceof HttpError ? error : new HttpError(error.message, error.code || "ERR_BAD_RESPONSE", config, request, response, error)
        }
        try {
            buffer = decodeBody(buffer, encoding)
        }
        catch (error) {
            throw new HttpError(error.message, error.code || "ERR_BAD_RESPONSE", config, request, response, error)
        }
        response.data = responseData(buffer, config.responseType)
    }
    const validateStatus = config.validateStatus === undefined ? defaultValidateStatus : config.validateStatus
    if (!response.status || !validateStatus || validateStatus(response.status))
        return response
    throw new HttpError("Request failed with status code " + response.status, response.status >= 400 && response.status < 500 ? "ERR_BAD_REQUEST" : "ERR_BAD_RESPONSE", config, request, response)
}

// ---- interceptors and the methods

function interceptorList() {
    const handlers = []
    return {
        handlers,
        use(fulfilled, rejected) {
            handlers.push({ fulfilled, rejected })
            return handlers.length - 1
        },
        eject(id) {
            if (handlers[id])
                handlers[id] = null
        },
        clear() {
            handlers.length = 0
        },
    }
}

const interceptors = { request: interceptorList(), response: interceptorList() }

function request(configOrUrl, config) {
    config = typeof configOrUrl == "string" ? { ...config, url: configOrUrl } : { ...configOrUrl }
    config.method = (config.method || "get").toLowerCase()
    // the default names first (Accept, Content-Type): a header given with another case (accept) takes their name, as axios
    const given = plainHeaders(config.headers)
    config.headers = { Accept: DEFAULT_ACCEPT, "Content-Type": undefined }
    for (const [name, value] of Object.entries(given))
        setHeader(config.headers, name, value)
    if (config.headers["Content-Type"] === undefined)
        delete config.headers["Content-Type"]
    // request interceptors: the last added first (as axios); then the request; then the response ones in order
    let chain = Promise.resolve(config)
    for (const handler of [...interceptors.request.handlers].reverse())
        if (handler)
            chain = chain.then(handler.fulfilled, handler.rejected)
    let result = chain.then(dispatch)
    for (const handler of interceptors.response.handlers)
        if (handler)
            result = result.then(handler.fulfilled, handler.rejected)
    return result
}

const httpClient = {
    request,
    getUri,
    interceptors,
    HttpError,
    isHttpError: value => value?.isHttpError === true,
}
for (const method of ["get", "delete", "head", "options"])
    httpClient[method] = (url, config) => httpClient.request({ ...config, method, url, data: config?.data })
for (const method of ["post", "put", "patch"])
    httpClient[method] = (url, data, config) => httpClient.request({ ...config, method, url, data })

module.exports = httpClient
