const amqp = require('amqplib');
const { MongoClient } = require('mongodb');
const { Client } = require('@elastic/elasticsearch');

const RABBIT_URL = process.env.RABBIT_URL || 'amqp://guest:guest@localhost:5672';
const ELASTIC_URL = process.env.ELASTIC_URL || 'http://localhost:9200';
const MONGO_URL = process.env.MONGO_URL || 'mongodb://localhost:27017/commagg';

async function start() {
  const conn = await amqp.connect(RABBIT_URL);
  const ch = await conn.createChannel();
  await ch.assertQueue('logging', { durable: true });
  const mongo = new MongoClient(MONGO_URL);
  await mongo.connect();
  const logsColl = mongo.db().collection('logs');

  // try ES client
  let es;
  try {
    es = new Client({ node: ELASTIC_URL });
    await es.cluster.health();
    console.log('Elasticsearch connected');
  } catch (e) {
    console.warn('Elasticsearch not available, will store logs in MongoDB');
    es = null;
  }

  ch.consume('logging', async (msg) => {
    if (!msg) return;
    try {
      const doc = JSON.parse(msg.content.toString());
      doc.receivedAt = new Date().toISOString();
      if (es) {
        await es.index({ index: 'comm-logs', document: doc });
      } else {
        await logsColl.insertOne(doc);
      }
      ch.ack(msg);
    } catch (err) {
      console.error('log handler error', err);
      ch.nack(msg, false, true);
    }
  }, { noAck: false });

  console.log('Logging service started');
}

start().catch(e => { console.error(e); process.exit(1); });
