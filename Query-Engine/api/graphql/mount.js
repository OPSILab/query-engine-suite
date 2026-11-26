// /graphql on the express app (Apollo Server 5 through @as-integrations/express4): CORS (with the same exposed
// headers as the REST routes), the JSON body, the same auth as the REST routes - with disableAuth it lets everything
// through, otherwise it requires a valid token and sets the user's prefix / bucket on req.body, used by the resolvers
// to scope the results - then Apollo, with { req } as context.
// Apollo's CSRF prevention is on: a GET (or a POST that is not JSON) needs a header such as
// apollo-require-preflight: true; the frontend, the Apollo Sandbox and curl with Content-Type: application/json pass.
const express = require("express")
const cors = require("cors")
const { expressMiddleware } = require("@as-integrations/express4")

function mountGraphql(app, server, { auth, corsOptions } = {}) {
    app.use("/graphql",
        cors(corsOptions),
        express.json(),
        (req, res, next) => { req.isGraphql = true; next() },
        auth || ((req, res, next) => next()),
        expressMiddleware(server, { context: async ({ req }) => ({ req }) }))
}

module.exports = { mountGraphql }
