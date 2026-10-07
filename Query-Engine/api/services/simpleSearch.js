// Simple search on the original sources besides MinIO: the APIs polled by the Source-Connector
// (apiConnectorConfig.apiUrls, same format as in the Source-Connector config) and Orion, called live.
// Nothing here reads or writes the databases.
//
// config.simpleSearchOptions (see config.template.js):
//   minio / api / orion      which sources are searched (orion is off by default: every entity points to a whole
//                            dataset, downloaded - and mapped, with a mapID - at every search)
//   maxPages                 pages read from a paginated API
//   maxOrionEntities         Orion entities downloaded per search
//   maxResults               matching records returned by the live sources
//   timeout                  ms, per request
// An API with `simpleSearch: false` in apiUrls is never searched.
//
// Warnings ({ kind: "config" | "runtime", code, source?, message }) tell the caller what was not searched or is
// incomplete: limits() for the configuration, searchLive() also for the failures and truncations of this search.

const axios = require('axios')
const logger = require('percocologger')
const config = require('../../config')

const DEFAULTS = { minio: true, api: true, orion: false, maxPages: 20, maxOrionEntities: 20, maxResults: 1000, timeout: 30000 }
const ORION_PAGE = 1000

function options() {
    return { ...DEFAULTS, ...(config.simpleSearchOptions || {}) }
}

function apis() {
    return Array.isArray(config.apiConnectorConfig?.apiUrls) ? config.apiConnectorConfig.apiUrls : []
}

const warning = (kind, code, message, source) => source === undefined ? { kind, code, message } : { kind, code, source, message }

// What the configuration leaves out of the simple search. `collection` (minio / api / orion): the collection the
// warning is about, so that the frontend shows only those of the selected collections.
function limits() {
    const o = options()
    const warnings = []
    const add = (collection, w) => warnings.push({ ...w, collection })
    if (o.minio === false)
        add("minio", warning("config", "MINIO_DISABLED", "MinIO files are not searched (simpleSearchOptions.minio = false)"))
    if (o.api === false) {
        if (apis().length)
            add("api", warning("config", "API_DISABLED", "API sources are not searched (simpleSearchOptions.api = false)"))
    }
    else
        for (const api of apis())
            if (api.simpleSearch === false)
                add("api", warning("config", "API_EXCLUDED", `API "${api.name}" is not searched (simpleSearch: false)`, api.name))
    if (o.orion !== true)
        add("orion", warning("config", "ORION_DISABLED", "Orion sources are not searched (simpleSearchOptions.orion = false)"))
    return warnings
}

// ---- requests

function basicAuth(credentials) {
    return "Basic " + Buffer.from(credentials.username + ":" + credentials.password).toString("base64")
}

const tokens = {} // authUrl -> { token, expiry }

async function fetchToken(header) {
    const { authUrl, credentials, authProfile } = header
    let response
    if (authProfile === "basic")
        response = await axios.request({ method: (authUrl.requestType || "POST").toLowerCase(), url: authUrl.value, headers: { Authorization: basicAuth(credentials) }, timeout: options().timeout })
    else if (authProfile == "OAuth 2.0 Client Credentials Grant")
        response = await axios.post(authUrl.value,
            new URLSearchParams({ client_id: credentials.client_id, client_secret: credentials.client_secret, grant_type: "client_credentials" }).toString(),
            { headers: { "Content-Type": "application/x-www-form-urlencoded" }, timeout: options().timeout })
    else
        throw new Error("No auth profile detected")
    return {
        token: "Bearer " + response.data.access_token,
        expiry: response.data.expires_in ? Date.now() + response.data.expires_in * 1000 : null
    }
}

// Same headers the Source-Connector's apiConnector sends: bearer tokens (fixed or obtained from authUrl, cached
// until they expire) and basic auth with alwaysSend; plain string values are sent as they are.
async function apiHeaders(api) {
    const headers = {}
    for (const [name, header] of Object.entries(api.headers || {})) {
        if (typeof header === "string")
            headers[name] = header
        else if (header?.type === "bearerToken") {
            const fixedValid = header.value && (!header.expiry || Date.now() < new Date(header.expiry).getTime())
            if (fixedValid)
                headers[name] = header.value
            else {
                const key = header.authUrl?.value
                if (!tokens[key] || (tokens[key].expiry && Date.now() >= tokens[key].expiry))
                    tokens[key] = await fetchToken(header)
                headers[name] = tokens[key].token
            }
        }
        else if (header?.type === "basic" && header.alwaysSend)
            headers[name] = basicAuth(header.credentials)
    }
    return headers
}

function valueAt(obj, param) {
    if (Array.isArray(param))
        return param.reduce((value, p) => value?.[p], obj)
    return obj?.[param]
}

function formatDate(date, format) {
    if (format === "YYYY-MM-DD")
        return date.toISOString().split("T")[0]
    if (format === "millis")
        return date.getTime()
    // other formats: no value, as in apiConnector (axios leaves the parameter out)
}

const asItems = data => Array.isArray(data) ? data : [data]

// Pages of records of one API, read the way apiConnector polls it - from the beginning: batch values, every
// page of a paginated API (up to maxPages), an incremental API with its end date only (no start date).
async function* apiPages(api, warnings) {
    const o = options()
    const headers = await apiHeaders(api)
    const get = (url, params) => axios.get(url, { headers, params, timeout: o.timeout })

    if (api.batch) {
        const batch = (await get(api.batch.from)).data
        const values = (api.batch.pick ? batch[api.batch.pick] : batch).map(item => valueAt(item, api.batch.param))
        for (const value of values) {
            const url = api.url.replace("{batch}", value)
            yield { origin: url, items: asItems((await get(url)).data) }
        }
    }
    else if (api.pagination) {
        const p = api.pagination
        const hasNext = typeof p.condition === "function" ? p.condition : response => asItems(response.data).length > 0
        let offset = p.offset || 0
        for (let page = 1; ; page++) {
            const url = api.url + (api.url.includes("?") ? "&" : "?") + `${p.limitParam}=${p.limit}&${p.offsetParam}=${offset}`
            const response = await get(url)
            yield { origin: api.url, items: asItems(response.data) }
            if (!hasNext(response))
                break
            if (page >= o.maxPages) {
                warnings.push(warning("runtime", "API_TRUNCATED", `API "${api.name}": only the first ${o.maxPages} pages were searched (simpleSearchOptions.maxPages)`, api.name))
                break
            }
            offset += p.limit
        }
    }
    else if (api.incremental) {
        const ip = api.incrementalParams || {}
        const endDate = new Date()
        if (ip.endDateLogic != "inclusive")
            endDate.setDate(endDate.getDate() + 1)
        const params = { ...(ip.endDateParam ? { [ip.endDateParam]: formatDate(endDate, ip.endDateFormat) } : {}), ...(api.queryParams || {}) }
        yield { origin: api.url, items: asItems((await get(api.url, params)).data) }
    }
    else {
        const response = api.method?.toLowerCase() === "post"
            ? await axios.post(api.url, api.body, { headers, timeout: o.timeout })
            : await get(api.url)
        yield { origin: api.url, items: asItems(response.data) }
    }
}

// ---- Orion

let mapperToken // { token, expiry }

// Keycloak token for the data model mapper (password grant with authConfig.clientId / username / password, like
// the Source-Connector's updateJWT). Kept in memory only.
async function getMapperToken(force) {
    if (!force && mapperToken && (!mapperToken.expiry || Date.now() < mapperToken.expiry))
        return mapperToken.token
    const a = config.authConfig || {}
    const response = await axios.post(
        `${a.idmHost}/realms/${a.authRealm || "master"}/protocol/openid-connect/token`,
        new URLSearchParams({ grant_type: "password", client_id: a.clientId, username: a.username, password: a.password }).toString(),
        { headers: { "Content-Type": "application/x-www-form-urlencoded" }, timeout: options().timeout }
    )
    mapperToken = {
        token: response.data.access_token,
        expiry: response.data.expires_in ? Date.now() + (response.data.expires_in - 10) * 1000 : null
    }
    return mapperToken.token
}

function extractValue(ent, attr) {
    const v = ent?.[attr]
    if (v && typeof v === "object" && "value" in v)
        return v.value
    return v
}

function downloadURLOf(ent) {
    const attr = config.orion?.attrWithUrl || "datasetUrl"
    const url = extractValue(ent, attr) || ent?.[attr + ":value"] || ent?.value
    return typeof url === "string" ? url : undefined
}

async function orionEntities(max) {
    const o = options()
    if (config.orion?.useNgsiBroker) {
        const entities = asItems((await axios.get(config.orion.ngsiBrokerUrl, { timeout: o.timeout })).data)
        return { entities: entities.slice(0, max), more: entities.length > max }
    }
    const type = config.orion?.subscribeType || "DistributionDCAT-AP"
    const base = `${config.orion?.orionBaseUrl}/ngsi-ld/v1/entities?type=${encodeURIComponent(type)}`
    const entities = []
    for (let offset = 0; entities.length <= max; offset += ORION_PAGE) {
        const page = asItems((await axios.get(`${base}&limit=${ORION_PAGE}&offset=${offset}`, { timeout: o.timeout })).data)
        entities.push(...page)
        if (page.length < ORION_PAGE)
            break
    }
    return { entities: entities.slice(0, max), more: entities.length > max }
}

// The records of the dataset an entity points to: the file as it is, or - with a mapID - the datapoints the data
// model mapper produces from it (same calls as the Source-Connector's Orion notification handler).
async function* orionDatasetPages(ent, downloadURL) {
    const o = options()
    const mapID = ent.mapID || config.mapID
    if (!mapID) {
        const data = (await axios.get(downloadURL, { timeout: o.timeout })).data
        yield asItems(data?.data?.datapoints || data)
        return
    }
    const format = String(extractValue(ent, "format") || "xml").toLowerCase()
    const sourceDataType = format === "xml" ? "sdmx-xml" : format
    let yielded = false
    for (let attempt = 0; ; attempt++) {
        try {
            const auth = { Authorization: `Bearer ${await getMapperToken(attempt > 0)}` }
            let map
            try {
                map = await axios.get(config.getMapEndpoint || "http://localhost:5500/api/map", { params: { description: downloadURL }, headers: auth, timeout: o.timeout })
            }
            catch (error) {
                if (error.response?.status != 404)
                    throw error
            }
            const body = {
                sourceDataType,
                sourceDataURL: downloadURL,
                decodeOptions: { decodeFrom: sourceDataType },
                config: { NGSI_entity: false, ignoreValidation: true, writers: [], disableAjv: true, mappingReport: true, newSdmxDecode: !!map?.data }
            }
            const mapped = map?.data
                ? await axios.post(config.mapEndpoint, { ...body, mapDescription: downloadURL }, { headers: auth, timeout: o.timeout })
                : await axios.post(config.parseEndpoint, body, { headers: auth, timeout: o.timeout })
            const outputId = mapped.data[mapped.data.length - 1].MAPPING_REPORT.outputId
            let lastId
            for (let index = 0; ; index++) {
                const chunk = (await axios.get((config.sessionEndpoint || "http://localhost:5500/api/output?") + "id=" + outputId + "&lastId=" + lastId + "&index=" + index, { headers: auth, timeout: o.timeout })).data
                if (!Array.isArray(chunk) || !chunk.length)
                    return
                yielded = true
                yield chunk
                lastId = chunk[chunk.length - 1]?._id
            }
        }
        catch (error) {
            if (attempt == 0 && !yielded && (error.response?.status == 401 || error.response?.status == 403))
                continue // token refused: once more with a new one
            throw error
        }
    }
}

// ---- search

const matches = (item, value) => !value || (typeof item === "string" ? item : JSON.stringify(item) ?? "").includes(value)

// Records of the APIs (and of Orion, if enabled) containing `value`, shaped like the MinIO simple search results
// ({ raw, name, record }) with record.from = origin: they are public data (visibility.isApiRecord).
// which: { api, orion } - the live sources to search (the collections selected by the user), both by default
async function searchLive(value, warnings = [], which = { api: true, orion: true }) {
    const o = options()
    const results = []
    let full = false
    const add = (raw, name, origin, extra) => {
        if (full || !matches(raw, value))
            return
        if (results.length >= o.maxResults) {
            full = true
            warnings.push(warning("runtime", "MAX_RESULTS", `Only the first ${o.maxResults} results of the API / Orion sources are returned (simpleSearchOptions.maxResults)`))
            return
        }
        results.push({ raw, name, source: origin, record: { from: origin, ...extra } })
    }

    const searches = []
    if (o.api !== false && which.api !== false)
        for (const api of apis().filter(api => api.simpleSearch !== false))
            searches.push((async () => {
                try {
                    for await (const { origin, items } of apiPages(api, warnings)) {
                        for (const item of items)
                            add(item, api.name, origin, { api: api.name })
                        if (full)
                            break
                    }
                }
                catch (error) {
                    logger.error(`Simple search: API ${api.name} failed`, error.response?.status || error.message)
                    warnings.push(warning("runtime", "API_ERROR", `API "${api.name}" could not be searched (${error.response?.status ? "HTTP " + error.response.status : error.message})`, api.name))
                }
            })())

    if (o.orion === true && which.orion !== false)
        searches.push((async () => {
            let listed
            try {
                listed = await orionEntities(o.maxOrionEntities)
            }
            catch (error) {
                logger.error("Simple search: Orion entities could not be read", error.message)
                warnings.push(warning("runtime", "ORION_ERROR", `Orion could not be searched (${error.response?.status ? "HTTP " + error.response.status : error.message})`, "orion"))
                return
            }
            if (listed.more)
                warnings.push(warning("runtime", "ORION_TRUNCATED", `Only the first ${o.maxOrionEntities} Orion entities were searched (simpleSearchOptions.maxOrionEntities)`, "orion"))
            for (const ent of listed.entities) {
                const id = ent.id || ent["@id"]
                const format = extractValue(ent, "format")
                const downloadURL = downloadURLOf(ent)
                if ((format && String(format).toLowerCase() != "xml") || !downloadURL || full)
                    continue // same entities the Source-Connector ingests
                try {
                    for await (const items of orionDatasetPages(ent, downloadURL)) {
                        for (const item of items)
                            add(item, id, downloadURL, { orion: id })
                        if (full)
                            break
                    }
                }
                catch (error) {
                    logger.error(`Simple search: Orion entity ${id} failed`, error.response?.status || error.message)
                    warnings.push(warning("runtime", "ORION_ERROR", `Orion entity ${id} could not be searched (${error.response?.status ? "HTTP " + error.response.status : error.message})`, id))
                }
            }
        })())

    await Promise.all(searches)
    return results
}

module.exports = { limits, searchLive, apiHeaders, apiPages, options, _reset: () => { for (const k in tokens) delete tokens[k]; mapperToken = undefined } }
