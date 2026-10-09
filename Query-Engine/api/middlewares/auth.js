const config = require('../../config')
const axios = require('axios');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');
const authConfig = config.authConfig
const keycloakServerURL = authConfig.idmHost;
const realm = authConfig.authRealm;
const clientID = authConfig.clientId;
const clientSecret = authConfig.secret;
const { parseJwt } = require("../../utils/common")
const logger = require('percocologger')

function deniedQuery(query, bucketName, prefix) {
    //if (req.params.bucketName && req.params.objectName)
    //    logger.debug(req.body.bucketName , req.params.bucketName , req.body.prefix , req.params.objectName.split("/")[0] + "/" + req.params.objectName.split("/")[1])
    let analyzedQuery
    try {
        if (query) {
            analyzedQuery = query.split(" ").filter(str => str !== '').join(' ')
            let requestedBucketname = analyzedQuery.split("FROM")[1].split(" ")[1]
            if (requestedBucketname != bucketName && requestedBucketname != "public-data" && requestedBucketname != "publicdata" && requestedBucketname != "sources")
                return true
            analyzedQuery = query.split("name = '")[1]
            if (!analyzedQuery.startsWith(prefix) && requestedBucketname != "public-data" && requestedBucketname != "publicdata" && requestedBucketname != "sources")
                return true
        }
    }
    catch (error) {
        logger.error(error)
        return true
    }
}

// constant time comparison of the admin token
function sameSecret(given, expected) {
    if (typeof given !== "string")
        return false
    const a = crypto.createHash("sha256").update(given).digest()
    const b = crypto.createHash("sha256").update(expected).digest()
    return crypto.timingSafeEqual(a, b)
}

module.exports = {
    auth: async (req, res, next) => {

        console.debug({ headers: req.headers })

        if (!req.body)
            req.body = {}

        if (req.body.file)
            req.body = JSON.parse(req.body.file)

        if (!req.headers.visibility)
            req.headers.visibility = "private"

        // req.isGraphql (set in index.js): body.query is a GraphQL document there, not SQL - the SQL checks don't apply
        if (!req.isGraphql && req.headers.visibility == "public" && req?.body?.query && !req?.body?.query?.toLowerCase().includes("public-data"))
            return res.status(400).send("If you are requesting for public files you must search in public-data bucket");

        if (!req.isGraphql && req?.body?.query?.toLowerCase().includes("select * from public-data"))
            req.body.query = req.body.query.replace("SELECT * FROM public-data", "SELECT * FROM publicdata")

        if (authConfig.disableAuth) {
            req.body.bucketName = config.minioConfig.defaultBucket || "defaultbucket"
            req.body.prefix = config.group = config.minioConfig.defaultInputFolderName
            next()
        }

        else {
            let authHeader = req.headers.authorization;

            if (authHeader) {
                if (authHeader && !authHeader.startsWith("Bearer"))
                    authHeader = "Bearer " + authHeader

                const jwtToken = authHeader.split(' ')[1];
                let verifiedToken
                if (authConfig.publicKeys) {
                    let authenticated = false
                    let error
                    for (let publicKey of authConfig.publicKeys)
                        try {
                            verifiedToken = jwt.verify(jwtToken, //Buffer.from(
                                publicKey
                                //, 'base64').toString()
                                //-------//
                                , { algorithms: ['RS256'] })
                            authenticated = true
                            break
                        }
                        catch (err) {
                            if (err.message == "invalid token" || err.message == "jwt expired" || err.message == "jwt malformed")
                                return res.sendStatus(403);
                            else {
                                logger.warn("Still trying to validate token... ", err.message)
                                error = err
                            }
                        }
                    if (!authenticated) {
                        logger.error(error)
                        if (error.message == "invalid token" || error.message == "jwt expired" || error.message == "jwt malformed")
                            return res.sendStatus(403);
                        else
                            return res.sendStatus(500);
                    }
                }
                else {
                    try {
                        verifiedToken = jwt.verify(jwtToken, //Buffer.from(
                            authConfig.publicKey
                            //, 'base64').toString()
                            //-------//
                            , { algorithms: ['RS256'] })
                    }
                    catch (error) {

                        logger.error(error)
                        if (error.message == "invalid token" || error.message == "jwt expired" || error.message == "jwt malformed")
                            return res.sendStatus(403);
                        else
                            return res.sendStatus(500);
                    }
                }


                if (authConfig.introspect) {
                    const introspectionEndpoint = `${keycloakServerURL}/realms/${realm}/protocol/openid-connect/token/introspect`;

                    const data = new URLSearchParams();
                    data.append('token', jwtToken);
                    data.append('client_id', clientID);
                    data.append('client_secret', clientSecret);

                    axios.post(introspectionEndpoint, data)
                        .then(response => {
                            if (response.data.active) {
                                logger.info('Token valid:', response.data);
                                next();
                            } else {
                                logger.error('Token not valid.');
                                return res.sendStatus(403);
                            }
                        })
                        .catch(error => {
                            logger.error(error.response.data)
                            logger.error('Errore during token verify:', error.message);
                            return res.sendStatus(500);
                        });
                }
                else {

                    const decodedToken = verifiedToken || parseJwt(jwtToken)

                    if ((decodedToken.azp == authConfig.clientId) && ((decodedToken.exp * 1000) > Date.now())) {

                        if (config.authConfig.userInfoEndpoint)
                            try {
                                let data = (await axios.get(config.authConfig.userInfoEndpoint, { headers: { "Authorization": authHeader } })).data
                                let { pilot, username, email } = data
                                req.body.bucketName = pilot.toLowerCase() //+ "/" + email + "/" + config.minioConfig.defaultInputFolderName//{pilot, email}
                                req.body.prefix = (email || username) + "/" + config.minioConfig.defaultInputFolderName
                                config.group = email || username
                                logger.debug(req.body.prefix)
                            }
                            catch (error) {
                                logger.error(error?.toString())
                                logger.error(error?.response?.data || error?.response)
                                req.body.bucketName = config.minioConfig.defaultBucket || "defaultbucket"
                                req.body.prefix = decodedToken.email
                                config.group = decodedToken.email
                            }
                        else {
                            req.body.bucketName = config.minioConfig.defaultBucket || "defaultbucket"
                            req.body.prefix = decodedToken.email
                            config.group = decodedToken.email
                        }

                        if (config.enableQueryControl && !req.isGraphql)
                            if (!deniedQuery(req.body.query, req.body.bucketName, req.body.prefix))
                                next()
                            else
                                return res.status(403).send("Available bucketname is " + req.body.bucketName + " and you tried to access " + req.body.query.split("FROM ")[1].split(" ") + ".\nAvailable prefix is " + req.body.prefix + " and you tried to access this object " + req.body.query.split("name = '")[1].split("'"));
                        else
                            next();
                    }
                    else
                        return res.sendStatus(403);
                }
            }
            else
                return res.sendStatus(401);
        }
    },

    // After `auth`, for the endpoints that change the Query-Engine itself (cache reset / backup / restore): with
    // authentication on, the token must also have one of authConfig.adminRoles (Keycloak realm roles or roles of
    // the clientId client). adminRoles empty / missing: any authenticated user.
    // disableAuth: there is no user, so these endpoints are closed (403) unless authConfig.adminToken is set and sent
    // in the X-Admin-Token header (e.g. resetBackup deletes the cache history: not for anyone who reaches the API).
    adminOnly: (req, res, next) => {
        if (authConfig.disableAuth) {
            const adminToken = typeof authConfig.adminToken === "string" ? authConfig.adminToken : ""
            if (!adminToken)
                return res.status(403).send("Cache management is disabled while authConfig.disableAuth is true: set authConfig.adminToken and send it in the X-Admin-Token header")
            if (!sameSecret(req.headers["x-admin-token"], adminToken))
                return res.status(403).send("X-Admin-Token missing or wrong")
            return next()
        }
        const required = Array.isArray(authConfig.adminRoles) ? authConfig.adminRoles.filter(r => typeof r === "string" && r) : []
        if (!required.length)
            return next()
        const token = (req.headers.authorization || "").replace(/^Bearer\s+/i, "")
        const decoded = jwt.decode(token) || {}
        const roles = [
            ...(decoded.realm_access?.roles || []),
            ...(decoded.resource_access?.[authConfig.clientId]?.roles || [])
        ]
        if (required.some(role => roles.includes(role)))
            return next()
        return res.status(403).send(`One of these roles is required: ${required.join(", ")}`)
    }
};
