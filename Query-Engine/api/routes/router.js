const express = require("express")
const controller = require("../controllers/controller.js")
const router = express.Router()
const { auth } = require("../middlewares/auth.js")
const { bodyCheck } = require('../../utils/common.js')
const mongoose = require('mongoose');

router.post(encodeURI("/query"), auth, bodyCheck, controller.query)//, controller.queryMongo)
router.get(encodeURI("/query"), auth, controller.queryMongo)
router.get(encodeURI("/keys"), auth, controller.getKeys)
router.get(encodeURI("/values"), auth, controller.getValues)
router.get(encodeURI("/entries"), auth, controller.getEntries)
router.get(encodeURI("/minio/listObjects"), auth, controller.minioListObjects)
router.get('/manage-collections', (req, res) => {
    res.send(`
        <html>
        <head>
            <title>Delete Collections</title>
        </head>
        <body>
            <h1>Delete Collections</h1>
            <button onclick="deleteCollection('dimensions')">Delete Dimensions</button>
            <button onclick="deleteCollection('querymap')">Delete QueryMap</button>

            <div id="status" style="margin-top:20px;color:green;"></div>

            <script>
                async function deleteCollection(collectionName) {
                    if (!confirm('Sei sicuro di voler cancellare la collection ' + collectionName + '?')) return;
                    const res = await fetch('/delete-collection', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ collectionName })
                    });
                    const data = await res.json();
                    document.getElementById('status').innerText = data.message;
                }
            </script>
        </body>
        </html>
    `);
});

// Endpoint per cancellare la collection
router.post('/delete-collection', async (req, res) => {
    const { collectionName } = req.body;

    if (!collectionName || !['dimensions', 'querymap'].includes(collectionName)) {
        return res.status(400).json({ message: 'Collection non valida' });
    }

    try {
        // Cancella la collection
        await mongoose.connection.dropCollection(collectionName);
        return res.json({ message: `Collection '${collectionName}' cancellata con successo.` });
    } catch (err) {
        if (err.codeName === 'NamespaceNotFound') {
            return res.json({ message: `Collection '${collectionName}' non esiste.` });
        }
        console.error(err);
        return res.status(500).json({ message: 'Errore durante la cancellazione' });
    }
});

module.exports = router
