const { connect } = require('./rabbit');
const mongoose = require('mongoose');
const { Delivery } = require('./models');

const RABBIT_URL = process.env.RABBIT_URL || 'amqp://guest:guest@localhost:5672';
const MONGO_URL = process.env.MONGO_URL || 'mongodb://localhost:27017/commagg';
const FAIL_RATE = parseFloat(process.env.FAIL_RATE || '0.2'); // simulate failures

async function start() {
  await mongoose.connect(MONGO_URL);
  const { channel } = await connect(RABBIT_URL);

  channel.consume('routing', async (msg) => {
    if (!msg) return;
    const headers = msg.properties.headers || {};
    const trace_id = headers.trace_id || null;
    const payload = JSON.parse(msg.content.toString());
    const { messageId } = payload;
    try {
      // idempotency: do not process if delivery exists
      const existing = await Delivery.findOne({ messageId });
      if (existing) {
        channel.ack(msg);
        return;
      }

      // simulate send with random failure
      const willFail = Math.random() < FAIL_RATE;
      const attempts = (payload.attempt || 0) + 1;

      if (willFail && attempts < 5) {
        // emit failure log
        channel.sendToQueue('logging', Buffer.from(JSON.stringify({
          timestamp: new Date().toISOString(),
          service: 'delivery',
          level: 'WARN',
          trace_id,
          message: 'attempt_failed',
          event: { messageId, attempt: attempts }
        })), { persistent: true });

        // exponential backoff in-process then requeue
        const backoffMs = 1000 * Math.pow(2, attempts - 1);
        setTimeout(() => {
          payload.attempt = attempts;
          channel.sendToQueue('routing', Buffer.from(JSON.stringify(payload)), { persistent: true, headers: { trace_id }});
        }, backoffMs);

        channel.ack(msg);
        return;
      }

      // final: success (or final failed attempt if attempts >=5 but succeed chance here)
      const status = willFail ? 'failed' : 'sent';
      await Delivery.create({
        messageId, channel: payload.channel, to: payload.to, body: payload.body,
        status, attempts, lastError: willFail ? 'simulated error' : null,
        sentAt: new Date()
      });

      channel.sendToQueue('logging', Buffer.from(JSON.stringify({
        timestamp: new Date().toISOString(),
        service: 'delivery',
        level: willFail ? 'ERROR' : 'INFO',
        trace_id,
        message: willFail ? 'final_failed' : 'delivered',
        event: { messageId, status, attempts }
      })), { persistent: true });

      channel.ack(msg);
    } catch (err) {
      console.error('delivery error', err);
      channel.nack(msg, false, true); // requeue on unexpected error
    }
  }, { noAck: false });

  console.log('Delivery worker started');
}

start().catch(e => { console.error(e); process.exit(1); });
