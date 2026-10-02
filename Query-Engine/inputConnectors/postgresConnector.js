process.postgreInit = "busy"
const logger = require('percocologger')
const { Client } = require('pg');
const config = require('../config')
const { postgreConfig, postgreReaderConfig } = config
const client = new Client(postgreConfig);
let readerClient;

function connectReader() {
    readerClient = new Client(postgreReaderConfig)

    readerClient.connect((err) => {
        if (err) {
            logger.error('PostgreSQL reader connection error:', err)
            process.postgreInit = "done"
            return
        }

        logger.info('PostgreSQL reader connected successfully')

        readerClient.query('SET statement_timeout = 100000', (err) => {
            if (err) {
                logger.error('Error setting statement timeout:', err)
                process.postgreInit = "done"
                return
            }
            logger.info('Statement timeout set to 100000 ms for reader client')

            process.postgreInit = "done"
        })
    })
}

function checkUserExists() {
    client.query(
        `SELECT 1 FROM pg_roles WHERE rolname = $1`,
        ['readeruser'],
        (err, result) => {
            if (err) {
                logger.error('Error checking user existence:', err)
                process.postgreInit = "done"
                return
            }

            if (result.rows.length > 0) {
                logger.info('User already exists')
                setUserPrivileges()
            } else {
                createUser()
            }
        }
    )
}

function createUser() {
    client.query(
        `CREATE USER readeruser WITH PASSWORD '${postgreReaderConfig.password}'`,
        (err) => {
            if (err) {
                logger.error('Error creating reader user:', err)
                process.postgreInit = "done"
                return
            }

            logger.info('Reader user created')
            setUserPrivileges()
        }
    )
}

function setUserPrivileges() {
    const queries = [
        `GRANT CONNECT ON DATABASE ${postgreReaderConfig.database} TO readeruser`,
        `GRANT USAGE ON SCHEMA public TO readeruser`,
        `GRANT SELECT ON ALL TABLES IN SCHEMA public TO readeruser`,
        `ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO readeruser`//,
        //`REVOKE SELECT ON users FROM readeruser`
    ]

    let index = 0

    function next() {
        if (index === queries.length) {
            logger.info('All reader privileges configured')
            connectReader()
            return
        }

        client.query(queries[index++], (err) => {
            if (err) {
                logger.error('Error executing privilege query:', err)
                process.postgreInit = "done"
                return
            }

            next()
        })
    }

    next()
}

client.connect((err) => {
    if (err) {
        logger.error('PostgreSQL connection error:', err)
        process.postgreInit = "done"
        return
    }

    checkUserExists()
})
const getReaderClient = () => readerClient
module.exports = getReaderClient/*{
    client, 
    getReaderClient: () => readerClient
};*/