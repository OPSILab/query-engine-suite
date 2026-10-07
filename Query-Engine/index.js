process.percocologger = require("./percocologger.config");
const common = require("./utils/common")
const config = common.checkConfig(require('./config'), require('./config.template'))
const { ApolloServer } = require('apollo-server-express');
const typeDefs = require('./api/graphql/typeDefs');
const resolvers = require('./api/graphql/resolvers');
const express = require('express');
const bodyParser = require('body-parser');
const app = express();
const port = config.port;
const mongoose = require("mongoose");
const logger = require('percocologger')
mongoose.connect(config.mongo, { useNewUrlParser: true }).then(() => {
    logger.info("Connected to mongo")
    const cors = require('cors');
    const routes = require("./api/routes/router")
    logger.info(config.queryAllowedExtensions);

    const server = new ApolloServer({
        typeDefs,
        resolvers,
        context: ({ req }) => ({ req })
    });
    server.start().then(() => {
        // Same auth as the REST routes: with disableAuth it lets everything through, otherwise it requires a valid
        // token and sets the user's prefix/bucket on req.body (used by the resolvers to scope the results).
        // express.json() first: Apollo is mounted before the global body parser below, and auth reads req.body.
        const { auth } = require("./api/middlewares/auth")
        app.use('/graphql', express.json(), (req, res, next) => { req.isGraphql = true; next() }, auth);
        server.applyMiddleware({ app, path: '/graphql' });
        // X-Query-Warnings: what the simple search did not search / returned incomplete (read by the frontend)
        app.use(cors({ exposedHeaders: ["X-Query-Warnings"] }));
        app.use(express.urlencoded({ extended: false }));
        app.use(bodyParser.json());
        app.use(config.basePath || "/api", routes);
        app.listen(port, () => { logger.info(`Server listens on http://localhost:${port}`); });
        logger.info(`Node.js version: ${process.version}`);
    });
})
