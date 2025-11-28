const amqp = require('amqplib');

async function connect(url) {
  const c = await amqp.connect(url);
  const ch = await c.createChannel();
  // assert queues
  await ch.assertQueue('routing', { durable: true });
  await ch.assertQueue('logging', { durable: true });
  return { connection: c, channel: ch };
}

module.exports = { connect };
