const mongoose = require('mongoose');
const { Schema } = mongoose;

const DeliverySchema = new Schema({
  messageId: { type: String, unique: true, required: true },
  channel: String,
  to: String,
  body: String,
  status: String,
  attempts: Number,
  lastError: String,
  sentAt: Date,
  createdAt: { type: Date, default: Date.now }
});
const Delivery = mongoose.model('Delivery', DeliverySchema);

module.exports = { Delivery };
