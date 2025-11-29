// logging/index.js
const { MongoClient } = require('mongodb');
const { Client } = require('@elastic/elasticsearch');
const { connectWithRetry } = require('./rabbit');

const RABBIT_URL = process.env.RABBIT_URL || 'amqp://guest:guest@rabbitmq:5672';
const ELASTIC_URL = process.env.ELASTIC_URL || 'http://elastic:9200';
const MONGO_URL = process.env.MONGO_URL || 'mongodb://mongo:27017/commagg';

// how long (ms) to wait/poll ES before giving up on initial startup
const ES_WAIT_TIMEOUT_MS = parseInt(process.env.ES_WAIT_TIMEOUT_MS || '120000'); // 2 min
const ES_RETRY_INTERVAL_MS = parseInt(process.env.ES_RETRY_INTERVAL_MS || '30000'); // 30s

function wait(ms) { return new Promise(r => setTimeout(r, ms)); }

async function isEsHealthy(client) {
  try {
    const res = await client.cluster.health({ timeout: '5s' });
    const status = (res && res.body && res.body.status) ? res.body.status :
                   (res && res.statusCode ? (res.statusCode === 200 ? 'yellow' : null) : null);
    return status === 'yellow' || status === 'green';
  } catch (err) {
    return false;
  }
}

async function tryConnectEsWithTimeout(elasticUrl, timeoutMs = ES_WAIT_TIMEOUT_MS) {
  const client = new Client({ node: elasticUrl });
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await isEsHealthy(client)) return client;
    await wait(2000);
  }
  return null;
}

async function start() {
  console.log('Logging service starting...');

  // 1) Connect to RabbitMQ (with retry)
  const { connection, channel } = await connectWithRetry(RABBIT_URL);

  // ensure logging queue exists
  await channel.assertQueue('logging', { durable: true });
  console.log('AMQP logging queue asserted');

  // 2) Connect to MongoDB
  const mongo = new MongoClient(MONGO_URL, { useUnifiedTopology: true });
  await mongo.connect();
  const logsColl = mongo.db().collection('logs');
  console.log('MongoDB connected');

  // 3) Try to connect to Elasticsearch (initial attempt with timeout)
  let esClient = null;
  try {
    esClient = await tryConnectEsWithTimeout(ELASTIC_URL, ES_WAIT_TIMEOUT_MS);
    if (esClient) {
      console.log('Elasticsearch connected and healthy (initial)');
    } else {
      console.warn('Elasticsearch not healthy within startup timeout; will fallback to Mongo and retry in background');
      esClient = null;
      // start background reconnection attempts
      (async function backgroundEsReconnector() {
        while (!esClient) {
          try {
            console.log('Attempting background connect to Elasticsearch...');
            const c = new Client({ node: ELASTIC_URL });
            if (await isEsHealthy(c)) {
              esClient = c;
              console.log('Elasticsearch connected (background)');
              break;
            }
          } catch (err) {
            // ignore and retry
          }
          await wait(ES_RETRY_INTERVAL_MS);
        }
      })();
    }
  } catch (err) {
    console.warn('Elasticsearch init failed, will fallback to Mongo and retry in background:', err.message || err);
    esClient = null;
    (async function backgroundEsReconnector() {
      while (!esClient) {
        try {
          console.log('Attempting background connect to Elasticsearch...');
          const c = new Client({ node: ELASTIC_URL });
          if (await isEsHealthy(c)) {
            esClient = c;
            console.log('Elasticsearch connected (background)');
            break;
          }
        } catch (e) {}
        await wait(ES_RETRY_INTERVAL_MS);
      }
    })();
  }

  // 4) Start consuming the logging queue
  channel.consume('logging', async (msg) => {
    if (!msg) return;
    try {
      const doc = JSON.parse(msg.content.toString());
      doc.receivedAt = new Date().toISOString();

      if (esClient) {
        // try ES first; if ES index fails for this doc, fallback to Mongo
        try {
          await esClient.index({ index: 'comm-logs', document: doc });
        } catch (err) {
          console.error('Elasticsearch index error; falling back to Mongo for this doc:', err.message || err);
          try {
            await logsColl.insertOne(doc);
          } catch (mongoErr) {
            console.error('Failed to insert log into Mongo after ES error:', mongoErr);
            // Decide to nack or ack; avoid poison messages — ack to drop if DB insert fails too
          }
        }
      } else {
        // ES not available; store in Mongo
        await logsColl.insertOne(doc);
      }

      channel.ack(msg);
    } catch (err) {
      console.error('log handler error', err);
      // if parse fails -> ack to drop bad message; if other error -> nack but avoid infinite requeue
      try {
        JSON.parse(msg.content.toString()); // if this throws, payload is invalid
        // payload valid but handler error -> ack to drop (to avoid poison loop) OR nack with requeue=false
        channel.nack(msg, false, false);
      } catch (parseErr) {
        channel.ack(msg);
      }
    }
  }, { noAck: false });

  console.log('Logging service started and consuming "logging" queue');
}

// Start
start().catch(e => {
  console.error('Fatal error in logging service start:', e);
  process.exit(1);
});
