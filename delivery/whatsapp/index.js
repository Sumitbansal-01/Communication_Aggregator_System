// delivery/email/index.js
const mongoose = require('mongoose');
const { v4: uuidv4 } = require('uuid');
const { connectWithRetry } = require('./rabbit');
const { Delivery } = require('./models');

const RABBIT_URL = process.env.RABBIT_URL || 'amqp://guest:guest@rabbitmq:5672';
const MONGO_URL  = process.env.MONGO_URL  || 'mongodb://mongo:27017/commagg';
const CHANNEL    = process.env.CHANNEL    || 'whatsapp';
const MAX_ATTEMPTS = parseInt(process.env.MAX_ATTEMPTS || '5');

async function start() {
  console.log('Starting delivery service for channel:', CHANNEL);
  await mongoose.connect(MONGO_URL, {});

  const { channel } = await connectWithRetry(RABBIT_URL);

  // ensure exchange + queue + binding exist
  await channel.assertExchange('routing', 'direct', { durable: true });
  await channel.assertQueue(`${CHANNEL}-queue`, { durable: true });
  await channel.bindQueue(`${CHANNEL}-queue`, 'routing', CHANNEL);

  // make sure logging queue exists too (we will publish logs)
  await channel.assertQueue('logging', { durable: true });

  channel.consume(`${CHANNEL}-queue`, async (msg) => {
    if (!msg) return;
    let payload;
    try { 
      payload = JSON.parse(msg.content.toString()); 
    } catch (e) {
      console.error('invalid payload', e);
      channel.ack(msg); 
      return;
    }

    const messageId = payload.messageId;
    const headers = (msg.properties && msg.properties.headers) || {};
    const trace_id = headers.trace_id || payload.trace_id || uuidv4();
    const parent_span_id = headers.span_id || payload.span_id || null;

    // idempotency: skip if already processed
    const existing = await Delivery.findOne({ messageId });
    if (existing) {
      console.log(`[${CHANNEL}] already processed messageId=${messageId} status=${existing.status}`);
      channel.ack(msg);
      return;
    }

    const attempt = (payload.attempt || 0) + 1;
    const delivery_span = uuidv4();

    // log DeliveryAttempt (fire-and-forget best-effort)
    try {
      const ev = {
        timestamp: new Date().toISOString(),
        service: `delivery-${CHANNEL}`,
        event: 'DeliveryAttempt',
        level: 'INFO',
        trace_id,
        span_id: delivery_span,
        parent_span_id,
        message: `attempting delivery #${attempt}`,
        payload: { messageId, attempt, channel: CHANNEL, to: payload.to }
      };
      channel.sendToQueue('logging', Buffer.from(JSON.stringify(ev)), { persistent: true });
    } catch(e){/* ignore logging errors */ }

    try {
      console.log(`[${CHANNEL}] sending messageId=${messageId} attempt=${attempt}`);

      await Delivery.create({
        messageId,
        channel: CHANNEL,
        to: payload.to,
        body: payload.body,
        status: 'sent',
        attempts: attempt,
        lastError: null,
        sentAt: new Date()
      });

      // success log
      try {
        const ev = {
          timestamp: new Date().toISOString(),
          service: `delivery-${CHANNEL}`,
          event: 'DeliverySucceeded',
          level: 'INFO',
          trace_id,
          span_id: delivery_span,
          parent_span_id,
          message: 'delivered',
          payload: { messageId, attempt, channel: CHANNEL }
        };
        channel.sendToQueue('logging', Buffer.from(JSON.stringify(ev)), { persistent: true });
      } catch (e) {}

      channel.ack(msg);
      console.log(`[${CHANNEL}] delivered messageId=${messageId}`);
    } catch (err) {
      console.warn(`[${CHANNEL}] attempt failed messageId=${messageId} err=${err.message}`);

      // publish failure log
      try {
        const ev = {
          timestamp: new Date().toISOString(),
          service: `delivery-${CHANNEL}`,
          event: 'DeliveryAttemptFailed',
          level: 'WARN',
          trace_id,
          span_id: delivery_span,
          parent_span_id,
          message: err.message,
          payload: { messageId, attempt, channel: CHANNEL }
        };
        channel.sendToQueue('logging', Buffer.from(JSON.stringify(ev)), { persistent: true });
      } catch (e){}

      if (attempt < MAX_ATTEMPTS) {
        // requeue to routing exchange with exponential backoff (in-process)
        payload.attempt = attempt;
        // preserve trace/span linking: set parent to current delivery span
        const newHeaders = { trace_id, span_id: uuidv4(), parent_span_id: delivery_span };
        const backoffMs = 1000 * Math.pow(2, attempt - 1);
        console.log(`[${CHANNEL}] scheduling retry in ${backoffMs}ms for messageId=${messageId}`);
        setTimeout(() => {
          try {
            channel.publish('routing', CHANNEL, Buffer.from(JSON.stringify(payload)), { persistent: true, headers: newHeaders });
          } catch (e) { console.error('requeue failed', e); }
        }, backoffMs);
        channel.ack(msg);
      } else {
        // final failure stored
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
        // final failure log
        try {
          const ev = {
            timestamp: new Date().toISOString(),
            service: `delivery-${CHANNEL}`,
            event: 'DeliveryFailed',
            level: 'ERROR',
            trace_id,
            span_id: delivery_span,
            parent_span_id,
            message: 'final failure',
            payload: { messageId, attempt, channel: CHANNEL }
          };
          channel.sendToQueue('logging', Buffer.from(JSON.stringify(ev)), { persistent: true });
        } catch(e){}
        channel.ack(msg);
        console.log(`[${CHANNEL}] marked failed messageId=${messageId}`);
      }
    }
  }, { noAck: false });

  console.log(`[${CHANNEL}] consumer started and awaiting messages`);
}

start().catch(e => { console.error('fatal', e); process.exit(1); });
