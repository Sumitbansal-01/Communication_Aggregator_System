const amqp = require('amqplib');
async function connect(url) {
  const c = await amqp.connect(url);
  const ch = await c.createChannel();
  await ch.assertQueue('routing', { durable: true });
  await ch.assertQueue('logging', { durable: true });
  ch.prefetch(5);
  return { connection: c, channel: ch };
}
module.exports = { connect };
