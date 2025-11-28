const express = require('express');
const bodyParser = require('body-parser');
const { v4: uuidv4 } = require('uuid');
const mongoose = require('mongoose');
const crypto = require('crypto');
const { connect } = require('./rabbit');
const { Message } = require('./models');

const RABBIT_URL = process.env.RABBIT_URL || 'amqp://guest:guest@localhost:5672';
const MONGO_URL = process.env.MONGO_URL || 'mongodb://localhost:27017/commagg';

async function start() {
  await mongoose.connect(MONGO_URL);
  const { channel } = await connect(RABBIT_URL);

  const app = express();
  app.use(bodyParser.json());

  app.post('/api/v1/messages', async (req, res) => {
    try {
      const { idempotency_key, channel: ch, to, from, subject, body, metadata } = req.body;
      if (!ch || !to || !body) return res.status(400).json({ error: 'channel,to,body required' });

      // idempotency: prefer idempotency_key else body-hash
      const hash = crypto.createHash('sha256').update(JSON.stringify({ ch, to, from, subject, body })).digest('hex');
      let existing = null;
      if (idempotency_key) existing = await Message.findOne({ idempotencyKey: idempotency_key });
      if (!existing) existing = await Message.findOne({ hash });

      if (existing) {
        return res.status(200).json({ messageId: existing.messageId, status: existing.status, info: 'duplicate' });
      }

      const messageId = uuidv4();
      const trace_id = uuidv4();

      await Message.create({ messageId, idempotencyKey: idempotency_key, hash, channel: ch, status: 'queued' });

      const msg = {
        messageId, channel: ch, to, from, subject, body, metadata, createdAt: new Date().toISOString(), attempt: 0
      };
      // publish routing message
      channel.sendToQueue('routing', Buffer.from(JSON.stringify(msg)), { persistent: true, headers: { trace_id }});
      // publish log event
      channel.sendToQueue('logging', Buffer.from(JSON.stringify({
        timestamp: new Date().toISOString(),
        service: 'task-router',
        level: 'INFO',
        trace_id,
        message: 'enqueued',
        event: { messageId, channel: ch, to }
      })), { persistent: true });

      res.status(202).json({ messageId, status: 'queued', trace_id });
    } catch (err) {
      console.error(err);
      res.status(500).json({ error: 'server error' });
    }
  });

  app.get('/api/v1/messages/:id', async (req, res) => {
    const m = await Message.findOne({ messageId: req.params.id });
    if (!m) return res.status(404).json({ error: 'not found' });
    res.json({ messageId: m.messageId, status: m.status, attempts: m.attempts });
  });

  app.listen(3000, () => console.log('Task Router listening on 3000'));
}

start().catch(e => { console.error(e); process.exit(1); });
