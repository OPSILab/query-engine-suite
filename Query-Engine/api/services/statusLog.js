// GET /api/statuslog: the failed HTTP requests of the Source-Connector (collection "statuslog", written by its
// utils/requestLog.js), newest first.
//   from / to   createdAt from / to (ISO dates; a date alone, YYYY-MM-DD, is that whole day for `to`)
//   url         part of the url (case insensitive), looked for in url and fullUrl
//   limit, skip a page (limit 1-1000); without limit: every matching record
// Answer { items, total, masked }. Unless req.adminView (auth.adminView: a token with one of authConfig.adminRoles),
// the credentials are hidden ("<masked>"; the keys stay): every value of the request headers, and the values of the
// sensitive fields (SENSITIVE: password, secret, token, api key...) in the query params, the url query strings, the
// body and the response data (JSON or form fields; a cut JSON text by pattern). E.g. a token request keeps client_id
// and grant_type, not client_secret, and its response expires_in, not access_token. The records in the database are
// not changed.
//
// Ignored urls (collection "statuslogignored", POST / DELETE /api/statuslog/ignored, admins only; and
// statusLog.ignoredUrls in the config, fixed: not restored by DELETE): without a url filter, the records whose url is
// one of them are left out (the date filters do not bring them back); with a url filter they are listed too, with
// ignored: true. GET /api/statuslog/ignored: the list, those of the config first (fromConfig: true), urls masked as
// above for non admins.
const config = require("../../config")
const StatusLog = require("../models/StatusLog")
const StatusLogIgnored = require("../models/StatusLogIgnored")
const { timeLimit, httpStatus } = require("./queryGuards")

const MASK = "<masked>"
const MAX_LIMIT = 1000
const DATE_ONLY = /^\d{4}-\d{2}-\d{2}$/
const escapeRegex = text => text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")

const badRequest = message => Object.assign(new Error(message), { status: 400 })

function parseDate(value, name, endOfDay) {
    if (value === undefined || value === "")
        return undefined
    const date = new Date(value)
    if (typeof value != "string" || isNaN(date))
        throw badRequest(`${name} must be a date (ISO 8601, e.g. 2026-11-02 or 2026-11-02T10:00:00Z)`)
    if (endOfDay && DATE_ONLY.test(value))
        date.setTime(date.getTime() + 24 * 60 * 60 * 1000 - 1) // the whole day
    return date
}

function filterOf(query) {
    const filter = {}
    const from = parseDate(query.from, "from")
    const to = parseDate(query.to, "to", true)
    if (from || to)
        filter.createdAt = { ...(from ? { $gte: from } : {}), ...(to ? { $lte: to } : {}) }
    if (query.url !== undefined && query.url !== "") {
        if (typeof query.url != "string")
            throw badRequest("url must be a text")
        const part = { $regex: escapeRegex(query.url), $options: "i" }
        filter.$or = [{ url: part }, { fullUrl: part }]
    }
    return filter
}

function pageOf(query) {
    if (query.limit === undefined)
        return undefined
    const limit = Number(query.limit)
    const skip = query.skip === undefined ? 0 : Number(query.skip)
    if (!Number.isInteger(limit) || limit < 1 || limit > MAX_LIMIT || !Number.isInteger(skip) || skip < 0)
        throw badRequest(`limit must be 1-${MAX_LIMIT}, skip >= 0`)
    return { limit, skip }
}

// The headers with their values hidden. Saved as JSON text when MongoDB refused their field names: parsed first.
function maskHeaders(headers) {
    if (headers === undefined || headers === null)
        return headers
    if (typeof headers == "string")
        try {
            headers = JSON.parse(headers)
        }
        catch {
            return MASK
        }
    if (typeof headers != "object" || Array.isArray(headers))
        return MASK
    return Object.fromEntries(Object.keys(headers).map(key => [key, MASK]))
}

// The names of the fields whose values are hidden: password, secret, token (access_token, refresh_token...),
// api key, authorization / credentials, cookie, session, signature, private key
const SENSITIVE = /pass(word|wd)?|secret|token|api[-_]?key|auth|credential|cookie|session|signature|private/i

// An object / array with the values of its sensitive fields hidden, at any depth
function maskSensitive(value) {
    if (Array.isArray(value))
        return value.map(maskSensitive)
    if (value && typeof value == "object")
        return Object.fromEntries(Object.entries(value).map(([key, v]) => [key, SENSITIVE.test(key) ? MASK : maskSensitive(v)]))
    return value
}

// "a=1&client_secret=x": the values of the sensitive fields hidden, the rest as it was (encoding included)
function maskQueryString(text) {
    return text.split("&").map(pair => {
        const at = pair.indexOf("=")
        if (at < 0)
            return pair
        let key = pair.slice(0, at)
        try {
            key = decodeURIComponent(key.replace(/\+/g, " "))
        }
        catch { }
        return SENSITIVE.test(key) ? pair.slice(0, at + 1) + MASK : pair
    }).join("&")
}
const FORM = /^[^=&\s{[]+=[^&\s]*(&[^=&\s]+=[^&\s]*)*$/

// A url with the sensitive values of its query string hidden
function maskUrl(url) {
    if (typeof url != "string" || !url.includes("?"))
        return url
    const at = url.indexOf("?")
    const hash = url.indexOf("#", at)
    const query = hash < 0 ? url.slice(at + 1) : url.slice(at + 1, hash)
    return url.slice(0, at + 1) + maskQueryString(query) + (hash < 0 ? "" : url.slice(hash))
}

// A body / response text: JSON (whole: parsed; cut: by pattern), form fields, or other text left as it is
function maskText(text) {
    if (typeof text != "string" || !text)
        return text
    try {
        const parsed = JSON.parse(text)
        return parsed && typeof parsed == "object" ? JSON.stringify(maskSensitive(parsed)) : text
    }
    catch { }
    if (FORM.test(text.trim()))
        return maskQueryString(text.trim())
    // a JSON text cut by the Source-Connector: "<sensitive key>": "<value>" (a string value, even if cut at its end)
    return text.replace(/("([^"\\]*)"\s*:\s*)"(?:[^"\\]|\\.)*("|$)/g, (match, prefix, key) => SENSITIVE.test(key) ? `${prefix}"${MASK}"` : match)
}

// What a non admin sees of a record
function masked(item) {
    const out = { ...item, headers: maskHeaders(item.headers) }
    for (const field of ["url", "fullUrl"])
        if (out[field] !== undefined)
            out[field] = maskUrl(out[field])
    if (out.params !== undefined)
        out.params = typeof out.params == "string" ? maskText(out.params) : maskSensitive(out.params)
    for (const field of ["body", "data"])
        if (out[field] !== undefined)
            out[field] = maskText(out[field])
    return out
}

// statusLog.ignoredUrls of the config: the strings, once each
function configIgnored() {
    const urls = config.statusLog?.ignoredUrls
    return Array.isArray(urls) ? [...new Set(urls.filter(url => typeof url == "string" && url.trim()))] : []
}

async function ignoredUrls() {
    const saved = (await StatusLogIgnored.collection.find({}, timeLimit()).toArray()).map(doc => doc.url).filter(url => typeof url == "string")
    return [...new Set([...configIgnored(), ...saved])]
}

async function list(query, adminView) {
    const filter = filterOf(query)
    const page = pageOf(query)
    const ignored = await ignoredUrls()
    const urlFilter = filter.$or !== undefined
    if (!urlFilter && ignored.length)
        filter.url = { $nin: ignored }
    let cursor = StatusLog.collection.find(filter, timeLimit()).sort({ createdAt: -1, _id: -1 })
    if (page)
        cursor = cursor.skip(page.skip).limit(page.limit)
    const [items, total] = await Promise.all([cursor.toArray(), StatusLog.collection.countDocuments(filter, timeLimit())])
    const ignoredSet = new Set(ignored)
    for (const item of items)
        if (ignoredSet.has(item.url))
            item.ignored = true
    return {
        items: adminView ? items : items.map(masked),
        total,
        masked: !adminView,
        // ignored urls left out of this answer (none with a url filter)
        ignoredExcluded: urlFilter ? 0 : ignored.length
    }
}

// The ignored urls: those of the config, then the others oldest first (an url in both: as in the config)
async function listIgnored(adminView) {
    const fromConfig = configIgnored()
    const docs = (await StatusLogIgnored.collection.find({}, timeLimit()).sort({ createdAt: 1, _id: 1 }).toArray())
        .filter(doc => !fromConfig.includes(doc.url))
        .map(({ _id, ...doc }) => doc)
    const urls = [...fromConfig.map(url => ({ url, fromConfig: true })), ...docs]
    return {
        urls: urls.map(doc => adminView ? doc : { ...doc, url: maskUrl(doc.url), ...(doc.by ? { by: MASK } : {}) }),
        masked: !adminView
    }
}

function urlOf(body) {
    const url = body?.url
    if (typeof url != "string" || !url.trim())
        throw badRequest("url is required (the url of the failed requests, as in their url field)")
    return url
}

// Who asked (the e-mail of the token, if any)
function requester(req) {
    try {
        const token = (req.headers.authorization || "").replace(/^Bearer\s+/i, "")
        const payload = token ? JSON.parse(Buffer.from(token.split(".")[1], "base64url").toString()) : {}
        return payload.email || payload.preferred_username
    }
    catch {
        return undefined
    }
}

async function ignore(url, by) {
    if (configIgnored().includes(url))
        return // already, in the config
    await StatusLogIgnored.collection.updateOne({ url }, { $setOnInsert: { url, createdAt: new Date(), ...(by ? { by } : {}) } }, { upsert: true })
}

async function restore(url) {
    if (configIgnored().includes(url))
        throw Object.assign(new Error("This url is ignored in the configuration of the Query-Engine (statusLog.ignoredUrls): remove it there"), { status: 409 })
    await StatusLogIgnored.collection.deleteMany({ url })
}

const handle = action => async (req, res) => {
    try {
        res.send(await action(req))
    }
    catch (error) {
        const status = httpStatus(error) // 400 bad filters, 504 too slow (queryOptions.mongoMaxTimeMS)
        res.status(status).send(status == 500 ? "Status log not available" : error.message)
    }
}

module.exports = {
    list,
    listIgnored,
    ignore,
    restore,
    maskHeaders,
    maskText,
    maskUrl,
    maskSensitive,
    MASK,

    endpoint: handle(req => list(req.query, req.adminView === true)),
    // GET /api/statuslog/ignored (adminView)
    ignoredEndpoint: handle(req => listIgnored(req.adminView === true)),
    // POST /api/statuslog/ignored { url } (auth, adminOnly): the list after the change
    ignoreEndpoint: handle(async req => {
        await ignore(urlOf(req.body), requester(req))
        return listIgnored(true)
    }),
    // DELETE /api/statuslog/ignored?url= (auth, adminOnly): the list after the change
    restoreEndpoint: handle(async req => {
        await restore(urlOf(req.query))
        return listIgnored(true)
    })
}
