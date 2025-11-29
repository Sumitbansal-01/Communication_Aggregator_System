// task-router/index.js (with retries)
const express = require("express");
const mongoose = require("mongoose");
const crypto = require("crypto");
const { v4: uuidv4 } = require("uuid");
const bodyParser = require("body-parser");
const { connectWithRetry } = require("./rabbit");
const Message = require("./models");

const app = express();
app.use(bodyParser.json());

const PORT = process.env.PORT || 3000;
const RABBIT_URL = process.env.RABBIT_URL || "amqp://guest:guest@rabbitmq:5672";
const MONGO_URL = process.env.MONGO_URL || "mongodb://mongo:27017/commagg";

// -----------------------------
// Utility
// -----------------------------
function hashBody(obj) {
    return crypto.createHash("sha256").update(JSON.stringify(obj)).digest("hex");
}

async function wait(ms) {
    return new Promise((res) => setTimeout(res, ms));
}

/**
 * retry helper with exponential backoff
 * fn - async function to call
 * attempts - max attempts
 * initialDelayMs - base delay
 */
async function retry(fn, attempts = 5, initialDelayMs = 2000) {
    let attempt = 0;
    while (true) {
        try {
            attempt++;
            return await fn();
        } catch (err) {
            if (attempt >= attempts) throw err;
            const delay = initialDelayMs
            console.warn(`Retry attempt ${attempt} failed: ${err.message}. Retrying in ${delay}ms`);
            await wait(delay);
        }
    }
}

// publish that retries on error
async function publishWithRetry(channel, exchange, routingKey, payload, opts = {}, attempts = 5) {
    const buf = Buffer.from(JSON.stringify(payload));
    return retry(
        async () => {
            // channel.publish rarely throws but channel might be closed; wrap in try/catch
            const ok = channel.publish(exchange, routingKey, buf, opts);
            // if publish returns false, it's a signal broker buffer is full — still consider it success
            // but if channel is closed it will throw; we catch via retry wrapper
            return ok;
        },
        attempts,
        2000
    );
}

// sendToQueue with retry
async function sendToQueueWithRetry(channel, queue, payload, opts = {}, attempts = 5) {
    const buf = Buffer.from(JSON.stringify(payload));
    return retry(
        async () => {
            const ok = channel.sendToQueue(queue, buf, opts);
            return ok;
        },
        attempts,
        2000
    );
}

// DB create with retry (but handle duplicate-key specially)
async function createMessageWithRetry(doc, attempts = 3) {
    return retry(
        async () => {
            return await Message.create(doc);
        },
        attempts,
        200
    );
}

// -----------------------------
// Start app
// -----------------------------
async function start() {
    console.log("Starting Task Router...");

    await mongoose.connect(MONGO_URL);
    console.log("Mongo connected");

    const { channel } = await connectWithRetry(RABBIT_URL);

    // Ensure exchange/queues exist
    await channel.assertExchange("routing", "direct", { durable: true });
    await channel.assertQueue("email-queue", { durable: true });
    await channel.assertQueue("sms-queue", { durable: true });
    await channel.assertQueue("whatsapp-queue", { durable: true });
    await channel.bindQueue("email-queue", "routing", "email");
    await channel.bindQueue("sms-queue", "routing", "sms");
    await channel.bindQueue("whatsapp-queue", "routing", "whatsapp");
    await channel.assertQueue("logging", { durable: true });

    // -----------------------------
    // Routes
    // -----------------------------

    app.post("/api/v1/messages", async (req, res) => {
        try {
            const trace_id = uuidv4();
            const entry_span = uuidv4();

            // log entry to logging queue (best-effort, non-blocking)
            const entryLog = {
                timestamp: new Date().toISOString(),
                service: "task-router",
                level: "INFO",
                trace_id,
                span_id: entry_span,
                message: "request_received",
            };
            // fire-and-forget, but attempt send
            try {
                channel.sendToQueue("logging", Buffer.from(JSON.stringify(entryLog)), { persistent: false });
            } catch (e) {
                /* ignore */
            }

            const { channel: messageChannel, to, from, subject, body, metadata } = req.body;

            // -------------------------
            // Validation
            // -------------------------
            if (!messageChannel) return res.status(400).json({ error: "channel is required" });
            if (!to) return res.status(400).json({ error: "to is required" });
            if (!body) return res.status(400).json({ error: "body is required" });

            const allowedChannels = ["email", "sms", "whatsapp"];
            if (!allowedChannels.includes(messageChannel)) {
                return res.status(400).json({ error: "Invalid channel" });
            }

            // -------------------------
            // Idempotency
            // -------------------------
            const bodyHash = hashBody({ messageChannel, to, from, subject, body, metadata });

            // Check existing by idempotencyKey OR bodyHash
            let existing = await Message.findOne({ hash: bodyHash });

            if (existing) {
                // RETURN SAME RESPONSE → DO NOT SEND AGAIN
                return res.status(200).json({
                    messageId: existing.messageId,
                    status: existing.status,
                    trace_id: existing.trace_id,
                    info: "duplicate_message_prevented"
                });
            }

            // -------------------------
            // Create new message entry (with retry)
            // -------------------------
            const messageId = uuidv4();

            const doc = {
                messageId,
                hash: bodyHash,
                channel: messageChannel,
                to,
                from,
                subject,
                body,
                metadata,
                status: "queued",
                trace_id
            };

            try {
                await createMessageWithRetry(doc, 3);
            } catch (err) {
                // if duplicate key (race) — fetch and return existing
                if (err && err.code === 11000) {
                    const found = await Message.findOne({
                        hash
                    });
                    return res.status(200).json({
                        messageId: found.messageId,
                        status: found.status,
                        trace_id: found.trace_id,
                        info: "duplicate"
                    });
                }
                console.error("DB create failed after retries:", err);
                return res.status(500).json({ error: "Could not persist message" });
            }

            // -------------------------
            // Publish message to routing exchange (with retry)
            // -------------------------
            const publish_span = uuidv4();
            const publishPayload = {
                messageId,
                to,
                from,
                subject,
                body,
                metadata,
                channel: messageChannel,
                attempt: 0,
                createdAt: new Date().toISOString(),
                trace_id,
                span_id: publish_span,
                parent_span_id: entry_span,
            };

            try {
                await publishWithRetry(channel, "routing", messageChannel, publishPayload, { persistent: true }, 5);
            } catch (err) {
                console.error("Failed to publish to routing exchange after retries:", err);
                // update message status to error
                await Message.updateOne({ messageId }, { $set: { status: "enqueue_failed", lastError: err.message } });
                return res.status(500).json({ error: "Failed to enqueue message" });
            }

            // -------------------------
            // Send a log to logging queue (with retry)
            // -------------------------
            const logDoc = {
                timestamp: new Date().toISOString(),
                service: "task-router",
                level: "INFO",
                trace_id,
                message: "message_queued",
                span_id: publish_span,
                parent_span_id: entry_span,
            };

            try {
                await sendToQueueWithRetry(channel, "logging", logDoc, { persistent: true }, 4);
            } catch (err) {
                console.warn("Failed to publish log after retries:", err);
                // not a hard failure — message was enqueued; continue
            }

            // -------------------------
            // Response
            // -------------------------
            return res.status(202).json({
                messageId,
                status: "queued",
                trace_id
            });

        } catch (err) {
            console.error("Error:", err);
            return res.status(500).json({ error: "Internal server error" });
        }
    });

    // -----------------------------
    // Start server
    // -----------------------------
    app.listen(PORT, () => {
        console.log(`Task Router running on port ${PORT}`);
    });
}

start().catch((err) => {
    console.error("Fatal error:", err);
    process.exit(1);
});
