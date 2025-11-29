// delivery/email/rabbit.js
const amqp = require('amqplib');

async function connectWithRetry(url, opts = {}) {
  const retries = opts.retries ?? 12;
  const initialDelay = opts.initialDelayMs ?? 1000;
  let attempt = 0;
  while (true) {
    try {
      attempt++;
      const conn = await amqp.connect(url);
      const ch = await conn.createChannel();
      console.log(`RabbitMQ connected (attempt ${attempt})`);
      return { connection: conn, channel: ch };
    } catch (err) {
      console.warn(`RabbitMQ connect attempt ${attempt} failed: ${err.message}`);
      if (attempt >= retries) throw err;
      const delay = initialDelay * Math.pow(2, attempt - 1);
      await new Promise(r => setTimeout(r, delay));
    }
  }
}

module.exports = { connectWithRetry };
