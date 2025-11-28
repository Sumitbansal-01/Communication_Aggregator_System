const mongoose = require('mongoose');
const { Schema } = mongoose;

const MessageSchema = new Schema({
  messageId: { type: String, unique: true },
  idempotencyKey: { type: String, index: { unique: true, sparse: true } },
  hash: String,
  channel: String,
  status: String,
  attempts: { type: Number, default: 0 },
  createdAt: { type: Date, default: Date.now }
});

module.exports = { Message: mongoose.model('Message', MessageSchema) };
