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
        server.applyMiddleware({ app, path: '/graphql' });
        app.use(cors());
        app.use(express.urlencoded({ extended: false }));
        app.use(bodyParser.json());
        app.use(config.basePath || "/api", routes);
        app.listen(port, () => { logger.info(`Server listens on http://localhost:${port}`); });
        logger.info(`Node.js version: ${process.version}`);
    });
})
