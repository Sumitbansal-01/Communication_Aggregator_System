const mongoose = require('mongoose');
const { Schema } = mongoose;

const DeliverySchema = new Schema({
  messageId: { type: String, unique: true },
  channel: String,
  to: String,
  body: String,
  status: String,
  attempts: Number,
  lastError: String,
  sentAt: Date
});
module.exports = { Delivery: mongoose.model('Delivery', DeliverySchema) };
