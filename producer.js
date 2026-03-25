const amqp = require("amqplib");

async function sendMessage() {
  const queue = "notification_queue";

  const connection = await amqp.connect("amqp://localhost");
  const channel = await connection.createChannel();

  await channel.assertQueue(queue);

  const message = {
    userId: 1,
    message: "Đơn hàng đã được tạo thành công!"   
  };

  channel.sendToQueue(queue, Buffer.from(JSON.stringify(message)));

  console.log("Đã gửi thông báo:", message);

  setTimeout(() => connection.close(), 500);
}

sendMessage();