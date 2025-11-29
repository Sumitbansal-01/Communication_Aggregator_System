// task-router/models.js
const mongoose = require("mongoose");

const MessageSchema = new mongoose.Schema(
  {
    messageId: { type: String, required: true, unique: true },

    // Idempotency
    hash: { type: String, unique: true },

    // Message fields
    channel: { type: String, required: true },       // "email" | "sms" | "whatsapp"
    to: { type: String, required: true },
    from: { type: String },
    subject: { type: String },
    body: { type: String, required: true },
    metadata: { type: Object },

    // Status: Task Router always sets to "queued"
    status: { type: String, default: "queued" },

    // Distributed tracing
    trace_id: { type: String, required: true },

    createdAt: { type: Date, default: Date.now },

    updatedAt: { type: Date, default: Date.now }
  },
  { collection: "messages" }
);

module.exports = mongoose.model("Message", MessageSchema);
