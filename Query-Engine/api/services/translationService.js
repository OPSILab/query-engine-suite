const axios = require('axios')
const IORedis = require('ioredis')
const common = require('../../utils/common')
const config = common.checkConfig(
  require('../../config'),
  require('../../config.template')
)

const TRANSLATOR_URL = config.TRANSLATOR_URL || 'http://127.0.0.1:5000'
const TRANSLATOR_URL_MBART = 'http://host.docker.internal:5555'
const REDIS_URL = config.REDIS_URL || null
const CACHE_TTL = parseInt(config.CACHE_TTL_SECONDS || '86400', 10)

let redis = null
if (REDIS_URL) {
  redis = new IORedis(REDIS_URL)
  redis.on('error', e => console.warn('Redis error:', e.message))
} else {
  // fallback in-memory cache
  var inMemoryCache = new Map()
}

/**
 * Normalize language code for LibreTranslate (it, en, fr, de, el, lt, pt, es, ...)
 */
function normLang (lang) {
  if (!lang) return 'en'
  return lang.toLowerCase()
}

async function getCached (key) {
  if (redis) {
    try {
      const val = await redis.get(key)
      return val ? JSON.parse(val) : null
    } catch (err) {
      console.warn('Redis get error:', err.message)
      return null
    }
  } else {
    return inMemoryCache.has(key) ? inMemoryCache.get(key) : null
  }
}

async function setCached (key, value) {
  if (redis) {
    try {
      await redis.set(key, JSON.stringify(value), 'EX', CACHE_TTL)
    } catch (err) {
      console.warn('Redis set error:', err.message)
    }
  } else {
    inMemoryCache.set(key, value)
    setTimeout(() => inMemoryCache.delete(key), CACHE_TTL * 1000)
  }
}

/**
 * Translate a single text using LibreTranslate
 * returns translated text or original on failure
 */
async function translateText (text, targetLang) {
  if (!text || typeof text !== 'string') return text
  const lang = normLang(targetLang)
  const cacheKey = `lt:${lang}:${text}`

  const cached = await getCached(cacheKey)
  if (cached) return cached

  try {
    //LibreTranslate

    const res = await axios.post(
      `${TRANSLATOR_URL}/translate`,
      {
        q: text,
        source: 'auto',
        target: lang,
        format: 'text'
      },
      { timeout: 30000 }
    )

    const translated =
      res.data && (res.data.translatedText || res.data[0]?.translatedText)
        ? res.data.translatedText || res.data[0].translatedText
        : null

    const out =
      translated || (res.data && typeof res.data === 'string' ? res.data : text)

    /*
    //EasyNMT(mBART-50)
    const resBART = await axios.post(
      `${TRANSLATOR_URL_MBART}/translate`,
      {
        text: text,
        target_lang: "en",
        beam_size: 5,
        autodetect_language: true
      },
      { timeout: 30000 }
    );

    const translated = resBART.data && (resBART.data.translatedText || resBART.data[0]?.translatedText) ? (resBART.data.translatedText || resBART.data[0].translatedText) : null;

    const out = translated || (resBART.data && typeof resBART.data === "string" ? resBART.data : text);
*/
    await setCached(cacheKey, out)
    return out
  } catch (err) {
    console.warn('Translate error (fallback to original):', err.message)
    return text
  }
}

/**
 * Translate only the selected fields of a datapoint object. Others fields remain intact.
 */
async function translateDataPoint (dp, targetLang) {
  if (!targetLang || targetLang.toLowerCase() === 'en') return dp

  // Fields we want to translate (add more if needed)
  const fieldsToTranslate = ['surveyName', 'surveyData', 'updateFrequency']

  const out = { ...dp }

  for (const f of fieldsToTranslate) {
    if (out[f]) {
      out[f] = await translateText(out[f], targetLang)
    }
  }

  if (out.meta && typeof out.meta === 'object' && out.meta.quality) {
    out.meta = {
      ...out.meta,
      quality: await translateText(out.meta.quality, targetLang)
    }
  }

  return out
}

/**
 * Translate array of datapoints with parallelization but respecting API latencies.
 */
async function translateDataPointsBatch (dataPoints, targetLang) {
  if (!targetLang || targetLang.toLowerCase() === 'en') return dataPoints

  const unique = new Map()

  // Controlla se un valore è un tipo speciale MongoDB
  function isMongoType (item) {
    return (
      Buffer.isBuffer(item) ||
      item?.constructor?.name === 'ObjectID' ||
      item?.constructor?.name === 'ObjectId'
    )
  }

  function normalizeMongoValue (item) {
    if (isMongoType(item)) {
      return item.toString()
    }
    return item
  }

  function collectStrings (item, visited) {
    // Gestione tipi primitivi
    if (!item || typeof item !== 'object') {
      if (typeof item === 'string' && item.trim().length > 0) {
        unique.set(item, null)
      }
      return
    }

    if (isMongoType(item)) return

    // Protezione riferimenti circolari
    if (visited.has(item)) return
    visited.add(item)

    for (const key in item) {
      if (Object.prototype.hasOwnProperty.call(item, key)) {
        collectStrings(item[key], visited)
      }
    }
  }

  // Raccolta stringhe da tradurre
  for (const dp of dataPoints) {
    collectStrings(dp, new Set())
  }

  const entries = Array.from(unique.keys())

  // Filtra stringhe vuote o non traducibili
  const toTranslate = entries.filter(
    txt =>
      txt &&
      txt.trim().length > 0 &&
      // Esclude stringhe di soli numeri/simboli (il tuo filtro originale)
      !/^[0-9\s\-_.:,]+$/.test(txt) &&
      // Esclude se la stringa è identica alla sua versione maiuscola
      txt !== txt.toUpperCase()
  )

  const translations = await Promise.all(
    toTranslate.map(txt => translateText(txt, targetLang))
  )

  toTranslate.forEach((orig, i) => {
    unique.set(orig, translations[i])
  })

  function applyTranslationsRecursively (item, visited) {
    // Gestione tipi primitivi
    if (!item || typeof item !== 'object') {
      if (typeof item === 'string') {
        return unique.get(item) || item
      }
      return item
    }

    if (isMongoType(item)) {
      return normalizeMongoValue(item)
    }

    // Protezione riferimenti circolari
    if (visited.has(item)) return item
    visited.add(item)

    if (Array.isArray(item)) {
      return item.map(el => applyTranslationsRecursively(el, visited))
    }

    const newObj = {}
    for (const key in item) {
      if (Object.prototype.hasOwnProperty.call(item, key)) {
        newObj[key] = applyTranslationsRecursively(item[key], visited)
      }
    }
    return newObj
  }

  // Applica traduzioni e normalizzazioni
  return dataPoints.map(dp => applyTranslationsRecursively(dp, new Set()))
}

module.exports = {
  translateText,
  translateDataPoint,
  translateDataPointsBatch
}
