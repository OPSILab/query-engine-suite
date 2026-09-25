process.postgreInit = "busy"
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

        readerClient.query('SET statement_timeout = 100000', (err) => {
            if (err) {
                logger.error('Error setting statement timeout:', err)
                process.postgreInit = "done"
                return
            }

            process.postgreInit = "done"
        })
    })
}

function checkUserExists() {
    client.query(
        `SELECT 1 FROM pg_roles WHERE rolname = $1`,
        ['readerUser'],
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
        `CREATE USER readerUser WITH PASSWORD '${postgreReaderConfig.password}'`,
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
        `GRANT CONNECT ON DATABASE ${postgreReaderConfig.database} TO readerUser`,
        `GRANT USAGE ON SCHEMA public TO readerUser`,
        `GRANT SELECT ON ALL TABLES IN SCHEMA public TO readerUser`,
        `ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO readerUser`,
        `REVOKE SELECT ON users FROM readerUser`
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