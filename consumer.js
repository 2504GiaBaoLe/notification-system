const amqp = require("amqplib");
const { createClient } = require("redis");
const winston = require("winston");
const logger = winston.createLogger({
  level: "info",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ level, message, timestamp }) => {
      return `${timestamp} [${level}] ${message}`;
    })
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: "consumer.log" })
  ]
});
const redisClient = createClient({
  url: "redis://localhost:6379"
});

redisClient.on("error", (err) => logger.error("Redis lỗi: " + err));

async function start() {
  await redisClient.connect();

  const queue = "notification_queue";

  const connection = await amqp.connect("amqp://localhost");
  const channel = await connection.createChannel();

  await channel.assertQueue(queue, { durable: true });

  logger.info("Consumer đang chạy...");

  channel.consume(queue, async (msg) => {
    if (msg !== null) {
      try {
        const data = JSON.parse(msg.content.toString());
        const message = data.message;
        const cacheKey = `msg:${message}`;
        const exists = await redisClient.get(cacheKey);

        if (exists) {
          logger.warn("Tin nhắn trùng, bỏ qua: " + message);
          channel.ack(msg);
          return;
        }

        await redisClient.setEx(cacheKey, 60, "processed");

        logger.info("Nhận thông báo: " + message);

        channel.ack(msg);

      } catch (err) {
        logger.error("Lỗi xử lý message: " + err.message);

        channel.nack(msg, false, false); 
      }
    }
  });
}

start().catch(err => {
  logger.error("Lỗi hệ thống: " + err.message);
});