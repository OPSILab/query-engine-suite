// GET /api/statuslog: the HTTP requests of the Source-Connector, failed and successful (collection "statuslog",
// written by its utils/requestLog.js), newest first.
//   from / to   createdAt from / to (ISO dates; a date alone, YYYY-MM-DD, is that whole day for `to`)
//   url         part of the url (case insensitive), looked for in url and fullUrl
//   outcome     error / success (records saved before the successes were logged have no outcome: errors)
//   limit, skip a page (limit 1-1000); without limit: every matching record
// Answer { items, total, masked }. Unless req.adminView (auth.adminView: a token with one of authConfig.adminRoles),
// the credentials are hidden ("<masked>"; the keys stay): every value of the request headers, and the values of the
// sensitive fields (SENSITIVE: password, secret, token, api key...) in the query params, the url query strings, the
// body and the response data (JSON or form fields; a cut JSON text by pattern). E.g. a token request keeps client_id
// and grant_type, not client_secret, and its response expires_in, not access_token. The records in the database are
// not changed.
//
// Ignored urls (collection "statuslogignored", POST / DELETE /api/statuslog/ignored, admins only; and
// statusLog.ignoredUrls / statusLog.ignoredPrefixes in the config, fixed: not restored by DELETE): an exact url, or a
// prefix (match: "prefix"; not "prefix": auth sets body.prefix) - the urls that begin with it, e.g. "https://dx-lab.eng.it/" for the whole domain (a prefix
// ending with "/" also covers the url without it: "https://dx-lab.eng.it"). Compared with the url field of the
// records (case sensitive). Without a url filter, the records whose url is ignored are left out (the date filters do
// not bring them back); with a url filter they are listed too, with ignored: true (and ignoredPrefix: the prefix, if
// that is why). GET /api/statuslog/ignored: the list, those of the config first (fromConfig: true), urls masked as
// above for non admins.
const config = require("../../config")
const StatusLog = require("../models/StatusLog")
const StatusLogIgnored = require("../models/StatusLogIgnored")
const { timeLimit, httpStatus } = require("./queryGuards")

const MASK = "<masked>"
const MAX_LIMIT = 1000
const OUTCOMES = ["error", "success"]
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
    if (query.outcome !== undefined && query.outcome !== "") {
        if (!OUTCOMES.includes(query.outcome))
            throw badRequest(`outcome must be one of ${OUTCOMES.join(", ")}`)
        filter.outcome = query.outcome == "success" ? "success" : { $ne: "success" }
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

// A prefix: at least the scheme and the host ("https://dx-lab.eng.it"), so that it cannot hide every record
const VALID_PREFIX = /^[a-z][a-z0-9+.-]*:\/\/[^/?#\s]+/i
const validPrefix = prefix => typeof prefix == "string" && VALID_PREFIX.test(prefix)

const strings = list => Array.isArray(list) ? [...new Set(list.filter(url => typeof url == "string" && url.trim()))] : []

// statusLog.ignoredUrls / ignoredPrefixes of the config: the strings, once each (prefixes without scheme and host:
// left out)
const configIgnored = () => strings(config.statusLog?.ignoredUrls)
const configPrefixes = () => strings(config.statusLog?.ignoredPrefixes).filter(validPrefix)
const fromConfig = (url, prefix) => (prefix ? configPrefixes() : configIgnored()).includes(url)

// The saved ones: { url } exact, { url, match: "prefix" } prefixes
const isPrefix = doc => doc.match === "prefix"
const kindFilter = (url, prefix) => prefix ? { url, match: "prefix" } : { url, match: { $ne: "prefix" } }

// Every ignored url and prefix (config and saved)
async function ignoredRules() {
    const saved = (await StatusLogIgnored.collection.find({}, timeLimit()).toArray()).filter(doc => typeof doc.url == "string")
    return {
        exact: [...new Set([...configIgnored(), ...saved.filter(doc => !isPrefix(doc)).map(doc => doc.url)])],
        prefixes: [...new Set([...configPrefixes(), ...saved.filter(isPrefix).map(doc => doc.url).filter(validPrefix)])]
    }
}

// The urls that begin with one of the prefixes (or equal one ending with "/" without it)
function prefixRegex(prefixes) {
    const alternatives = prefixes.flatMap(prefix => [escapeRegex(prefix),
        ...(prefix.endsWith("/") ? [escapeRegex(prefix.slice(0, -1)) + "$"] : [])])
    return new RegExp("^(?:" + alternatives.join("|") + ")")
}

// The prefix that ignores a url, if any (the longest)
const prefixOf = (url, prefixes) => typeof url != "string" ? undefined :
    prefixes.filter(prefix => url.startsWith(prefix) || (prefix.endsWith("/") && url === prefix.slice(0, -1))).sort((a, b) => b.length - a.length)[0]

async function list(query, adminView) {
    const filter = filterOf(query)
    const page = pageOf(query)
    const { exact, prefixes } = await ignoredRules()
    const urlFilter = filter.$or !== undefined
    if (!urlFilter && (exact.length || prefixes.length))
        filter.url = { ...(exact.length ? { $nin: exact } : {}), ...(prefixes.length ? { $not: prefixRegex(prefixes) } : {}) }
    let cursor = StatusLog.collection.find(filter, timeLimit()).sort({ createdAt: -1, _id: -1 })
    if (page)
        cursor = cursor.skip(page.skip).limit(page.limit)
    const [items, total] = await Promise.all([cursor.toArray(), StatusLog.collection.countDocuments(filter, timeLimit())])
    const ignoredSet = new Set(exact)
    for (const item of items) {
        const prefix = prefixOf(item.url, prefixes)
        if (ignoredSet.has(item.url) || prefix)
            item.ignored = true
        if (prefix && !ignoredSet.has(item.url))
            item.ignoredPrefix = prefix
    }
    return {
        items: adminView ? items : items.map(masked),
        total,
        masked: !adminView,
        // ignored urls and prefixes left out of this answer (none with a url filter)
        ignoredExcluded: urlFilter ? 0 : exact.length + prefixes.length
    }
}

// The ignored urls and prefixes: those of the config (urls, then prefixes), then the others oldest first (in both: as
// in the config)
async function listIgnored(adminView) {
    const docs = (await StatusLogIgnored.collection.find({}, timeLimit()).sort({ createdAt: 1, _id: 1 }).toArray())
        .filter(doc => typeof doc.url == "string" && !fromConfig(doc.url, isPrefix(doc)) && (!isPrefix(doc) || validPrefix(doc.url)))
        .map(({ _id, ...doc }) => doc)
    const urls = [...configIgnored().map(url => ({ url, fromConfig: true })), ...configPrefixes().map(url => ({ url, match: "prefix", fromConfig: true })), ...docs]
    return {
        urls: urls.map(doc => adminView ? doc : { ...doc, url: maskUrl(doc.url), ...(doc.by ? { by: MASK } : {}) }),
        masked: !adminView
    }
}

function urlOf(body) {
    const url = body?.url
    if (typeof url != "string" || !url.trim())
        throw badRequest("url is required (the url of the requests, as in their url field)")
    return url
}

// match: "prefix" / "exact" (default), in the body or the query string
function ruleOf(source) {
    const url = urlOf(source)
    if (source.match !== undefined && source.match !== "prefix" && source.match !== "exact")
        throw badRequest("match must be exact or prefix")
    const prefix = source.match === "prefix"
    if (prefix && !validPrefix(url))
        throw badRequest("A prefix must have at least the scheme and the host, e.g. https://dx-lab.eng.it/")
    return { url, prefix }
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

async function ignore(url, by, prefix = false) {
    if (fromConfig(url, prefix))
        return // already, in the config
    if (await StatusLogIgnored.collection.countDocuments(kindFilter(url, prefix)))
        return // already
    await StatusLogIgnored.collection.insertOne({ url, ...(prefix ? { match: "prefix" } : {}), createdAt: new Date(), ...(by ? { by } : {}) })
}

async function restore(url, prefix = false) {
    if (fromConfig(url, prefix))
        throw Object.assign(new Error(`This ${prefix ? "prefix" : "url"} is ignored in the configuration of the Query-Engine (statusLog.${prefix ? "ignoredPrefixes" : "ignoredUrls"}): remove it there`), { status: 409 })
    await StatusLogIgnored.collection.deleteMany(kindFilter(url, prefix))
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
    // POST /api/statuslog/ignored { url, match: "prefix" | "exact" } (auth, adminOnly): the list after the change
    ignoreEndpoint: handle(async req => {
        const { url, prefix } = ruleOf(req.body)
        await ignore(url, requester(req), prefix)
        return listIgnored(true)
    }),
    // DELETE /api/statuslog/ignored?url=&match=prefix (auth, adminOnly): the list after the change
    restoreEndpoint: handle(async req => {
        const { url, prefix } = ruleOf(req.query)
        await restore(url, prefix)
        return listIgnored(true)
    })
}
