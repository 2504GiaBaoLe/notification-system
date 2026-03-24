const express = require("express");
const amqp = require("amqplib");

const app = express();
app.use(express.json());

let channel;

async function connectQueue() {
  const connection = await amqp.connect("amqp://localhost");
  channel = await connection.createChannel();
  await channel.assertQueue("notification_queue");
}

app.post("/order", async (req, res) => {
  const order = req.body;

  const message = {
    userId: order.userId,
    message: "Đặt hàng thành công!"
  };

  channel.sendToQueue(
    "notification_queue",
    Buffer.from(JSON.stringify(message))
  );

  res.send("Tạo đơn hàng thành công!");
});

app.listen(3000, async () => {
  await connectQueue();
  console.log("Server đang chạy tại http://localhost:3000");
});