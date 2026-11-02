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
const StatusLog = require("../models/StatusLog")
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

async function list(query, adminView) {
    const filter = filterOf(query)
    const page = pageOf(query)
    let cursor = StatusLog.collection.find(filter, timeLimit()).sort({ createdAt: -1, _id: -1 })
    if (page)
        cursor = cursor.skip(page.skip).limit(page.limit)
    const [items, total] = await Promise.all([cursor.toArray(), StatusLog.collection.countDocuments(filter, timeLimit())])
    return {
        items: adminView ? items : items.map(masked),
        total,
        masked: !adminView
    }
}

module.exports = {
    list,
    maskHeaders,
    maskText,
    maskUrl,
    maskSensitive,
    MASK,

    endpoint: async (req, res) => {
        try {
            res.send(await list(req.query, req.adminView === true))
        }
        catch (error) {
            const status = httpStatus(error) // 400 bad filters, 504 too slow (queryOptions.mongoMaxTimeMS)
            res.status(status).send(status == 500 ? "Status log not available" : error.message)
        }
    }
}
