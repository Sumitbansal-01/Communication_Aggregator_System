// delivery/email/models.js
const mongoose = require('mongoose');
const { Schema } = mongoose;

const DeliverySchema = new Schema({
  messageId: { type: String, required: true, unique: true },
  channel: { type: String },
  to: { type: String },
  body: { type: String },
  status: { type: String }, // 'sent' | 'failed'
  attempts: { type: Number, default: 0 },
  lastError: { type: String },
  sentAt: { type: Date },
  createdAt: { type: Date, default: Date.now }
}, { collection: 'deliveries' });

module.exports = { Delivery: mongoose.model('Delivery', DeliverySchema) };
