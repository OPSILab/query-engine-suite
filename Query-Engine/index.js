process.percocologger = require("./percocologger.config");
const common = require("./utils/common")
const config = common.checkConfig(require('./config'), require('./config.template'))
const { ApolloServer } = require('@apollo/server');
const { mountGraphql } = require('./api/graphql/mount');
const typeDefs = require('./api/graphql/typeDefs');
const resolvers = require('./api/graphql/resolvers');
const express = require('express');
const bodyParser = require('body-parser');
const app = express();
const port = common.queryEnginePort(config); // queryEnginePort, else port
const mongoose = require("mongoose");
const logger = require('percocologger')
mongoose.connect(config.mongo, { useNewUrlParser: true }).then(() => {
    logger.info("Connected to mongo")
    // GraphQL datapoints cache: indexes, versions interrupted by a stop, pruning (in background)
    require("./api/services/queryCache").recover().catch(error => logger.error("Query cache recovery failed", error))
    const cors = require('cors');
    const routes = require("./api/routes/router")
    logger.info(config.queryAllowedExtensions);

    // Apollo Server 5 (@apollo/server): /graphql mounted by api/graphql/mount.js (CORS, JSON body, auth, CSRF)
    const server = new ApolloServer({ typeDefs, resolvers });
    server.start().then(() => {
        // X-Query-Warnings: what the simple search did not search / returned incomplete (read by the frontend)
        const corsOptions = { exposedHeaders: ["X-Query-Warnings"] }
        mountGraphql(app, server, { auth: require("./api/middlewares/auth").auth, corsOptions });
        app.use(cors(corsOptions));
        app.use(express.urlencoded({ extended: false }));
        app.use(bodyParser.json());
        app.use(config.basePath || "/api", routes);
        app.listen(port, () => { logger.info(`Server listens on http://localhost:${port}`); });
        logger.info(`Node.js version: ${process.version}`);
    });
})
