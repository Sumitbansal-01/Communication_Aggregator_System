const mongoose = require('mongoose');
const { v4: uuidv4 } = require('uuid');
const { connectWithRetry } = require('./rabbit');
const { Delivery } = require('./models');

const RABBIT_URL = process.env.RABBIT_URL || 'amqp://guest:guest@rabbitmq:5672';
const MONGO_URL = process.env.MONGO_URL || 'mongodb://mongo:27017/commagg';
const CHANNEL = process.env.CHANNEL || 'email';
const FAIL_RATE = parseFloat(process.env.FAIL_RATE || '0.2');
const MAX_ATTEMPTS = parseInt(process.env.MAX_ATTEMPTS || '5');

async function simulateSend(payload) {
  // simulate transient failures
  if (Math.random() < FAIL_RATE) {
    const e = new Error('simulated provider failure');
    e.transient = true;
    throw e;
  }
  // simulate success
  return true;
}

async function start() {
  console.log('Starting delivery service for channel:', CHANNEL);
  await mongoose.connect(MONGO_URL);
  const { channel } = await connectWithRetry(RABBIT_URL);

  channel.consume(`${CHANNEL}-queue`, async (msg) => {
    if (!msg) return;
    let payload;
    try {
      payload = JSON.parse(msg.content.toString());
    } catch (e) {
      console.error('invalid message payload', e);
      channel.ack(msg);
      return;
    }
    const messageId = payload.messageId;
    // idempotency: skip if already delivered
    const existing = await Delivery.findOne({ messageId });
    if (existing) {
      console.log(`[${CHANNEL}] already processed messageId=${messageId} status=${existing.status}`);
      channel.ack(msg);
      return;
    }

    const attempt = (payload.attempt || 0) + 1;
    try {
      console.log(\`[${CHANNEL}] sending messageId=\${messageId} attempt=\${attempt}\`);
      await simulateSend(payload);
      await Delivery.create({
        messageId,
        channel: CHANNEL,
        to: payload.to,
        body: payload.body,
        status: 'sent',
        attempts: attempt,
        sentAt: new Date()
      });
      // (optional) publish simple logging event to 'logging' queue
      try {
        const log = {
          timestamp: new Date().toISOString(),
          service: 'delivery-' + CHANNEL,
          level: 'INFO',
          trace_id: (msg.properties.headers||{}).trace_id || null,
          message: 'delivered',
          event: { messageId, channel: CHANNEL, attempt }
        };
        channel.sendToQueue('logging', Buffer.from(JSON.stringify(log)), { persistent: true });
      } catch (e) { /* ignore logging failures */ }

      channel.ack(msg);
      console.log(\`[${CHANNEL}] delivered messageId=\${messageId}\`);
    } catch (err) {
      console.warn(\`[${CHANNEL}] attempt failed messageId=\${messageId} err=\${err.message}\`);
      // publish failure log
      try {
        const log = {
          timestamp: new Date().toISOString(),
          service: 'delivery-' + CHANNEL,
          level: 'WARN',
          trace_id: (msg.properties.headers||{}).trace_id || null,
          message: 'attempt_failed',
          event: { messageId, channel: CHANNEL, attempt, error: err.message }
        };
        channel.sendToQueue('logging', Buffer.from(JSON.stringify(log)), { persistent: true });
      } catch (e) {}

      if (attempt < MAX_ATTEMPTS) {
        // schedule retry with exponential backoff (simple in-process)
        payload.attempt = attempt;
        const backoffMs = 1000 * Math.pow(2, attempt - 1);
        console.log(\`[${CHANNEL}] scheduling retry in \${backoffMs}ms for messageId=\${messageId}\`);
        setTimeout(() => {
          try {
            channel.sendToQueue('routing', Buffer.from(JSON.stringify(payload)), { persistent: true, headers: { trace_id: (msg.properties.headers||{}).trace_id || null } });
          } catch (e) { console.error('requeue failed', e); }
        }, backoffMs);
        channel.ack(msg);
      } else {
        await Delivery.create({
          messageId,
          channel: CHANNEL,
          to: payload.to,
          body: payload.body,
          status: 'failed',
          attempts: attempt,
          lastError: err.message,
          sentAt: new Date()
        });
        channel.ack(msg);
        console.log(\`[${CHANNEL}] marked failed messageId=\${messageId}\`);
      }
    }
  }, { noAck: false });

  console.log(\`[${CHANNEL}] consumer started and awaiting messages\`);
}

start().catch(e => { console.error('fatal', e); process.exit(1); });
