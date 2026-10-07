// Local HTTP server standing in for the external world of the simple search: polled APIs (OAuth token, batch,
// pagination, incremental, POST, failing), Orion (NGSI-LD entities + datasets) and the data model mapper with
// its Keycloak.
const express = require("express")

async function startFakeSources() {
    const app = express()
    app.use(express.json())
    app.use(express.urlencoded({ extended: false }))
    const calls = []
    app.use((req, res, next) => { calls.push({ method: req.method, path: req.path, query: { ...req.query }, headers: { ...req.headers }, body: req.body }); next() })

    const requireBearer = token => (req, res, next) => req.headers.authorization == "Bearer " + token ? next() : res.sendStatus(401)

    // ---- APIs
    app.post("/oauth/token", (req, res) => req.body.grant_type == "client_credentials" && req.body.client_id == "cid" && req.body.client_secret == "secret"
        ? res.send({ access_token: "api-token", expires_in: 3600 })
        : res.sendStatus(400))
    app.get("/plain", requireBearer("api-token"), (req, res) => res.send([{ city: "Rome", id: 1 }, { city: "Milan", id: 2 }]))
    app.get("/single", (req, res) => res.send({ city: "Rome", single: true }))
    app.post("/search", (req, res) => res.send([{ asked: req.body.q, city: "Rome" }]))
    app.get("/batch-list", (req, res) => res.send({ items: [{ ref: { id: "a" } }, { ref: { id: "b" } }] }))
    app.get("/batch/:id", (req, res) => res.send({ id: req.params.id, city: req.params.id == "a" ? "Rome" : "Paris" }))
    const PAGED = [{ n: 0 }, { n: 1 }, { n: 2 }, { n: 3 }, { n: 4, city: "Rome" }]
    app.get("/paged", (req, res) => res.send(PAGED.slice(+req.query.offset, +req.query.offset + +req.query.limit)))
    app.get("/incremental", (req, res) => res.send([{ params: req.query, city: "Rome" }]))
    app.get("/fail", (req, res) => res.sendStatus(500))
    app.get("/static-key", (req, res) => req.headers["x-api-key"] == "k" ? res.send([{ city: "Rome" }]) : res.sendStatus(403))

    // ---- Orion
    const base = () => `http://127.0.0.1:${server.address().port}`
    app.get("/ngsi-ld/v1/entities", (req, res) => {
        const all = [
            { id: "urn:e1", type: "DistributionDCAT-AP", format: { value: "XML" }, datasetUrl: { value: base() + "/dataset/1" } },
            { id: "urn:e2", type: "DistributionDCAT-AP", format: { value: "CSV" }, datasetUrl: { value: base() + "/dataset/2" } },
            { id: "urn:e3", type: "DistributionDCAT-AP", format: { value: "XML" }, datasetUrl: { value: base() + "/dataset/3" }, mapID: "map-1" },
            { id: "urn:e4", type: "DistributionDCAT-AP", datasetUrl: base() + "/dataset/4" }
        ]
        res.send(all.slice(+req.query.offset || 0, (+req.query.offset || 0) + (+req.query.limit || 1000)))
    })
    app.get("/dataset/1", (req, res) => res.send([{ city: "Rome", value: 1 }, { city: "Oslo", value: 2 }]))
    app.get("/dataset/2", (req, res) => res.send([{ city: "Rome", csv: true }]))
    app.get("/dataset/4", (req, res) => res.send({ data: { datapoints: [{ region: "Rome", value: 4 }] } }))

    // ---- Keycloak + data model mapper
    app.post("/realms/test/protocol/openid-connect/token", (req, res) => req.body.grant_type == "password" && req.body.username == "mapper-user"
        ? res.send({ access_token: "kc-token", expires_in: 300 })
        : res.sendStatus(401))
    app.get("/api/map", requireBearer("kc-token"), (req, res) => res.sendStatus(404))
    app.post("/api/parse", requireBearer("kc-token"), (req, res) => res.send([{ some: "report" }, { MAPPING_REPORT: { outputId: "out-1", source: req.body.sourceDataURL } }]))
    app.get("/api/output", requireBearer("kc-token"), (req, res) => {
        const chunks = [[{ _id: "d1", region: "Rome", value: 10 }, { _id: "d2", region: "Oslo", value: 11 }], [{ _id: "d3", region: "Rome", value: 12 }]]
        res.send(req.query.id == "out-1" ? chunks[+req.query.index] || [] : [])
    })

    let server
    await new Promise(resolve => { server = app.listen(0, "127.0.0.1", resolve) })
    return {
        url: base(),
        calls,
        callsTo: path => calls.filter(c => c.path == path),
        close: () => new Promise(resolve => server.close(resolve))
    }
}

module.exports = { startFakeSources }
