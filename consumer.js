const amqp = require("amqplib");

async function receiveMessage() {
  const queue = "notification_queue";

  const connection = await amqp.connect("amqp://localhost");
  const channel = await connection.createChannel();

  await channel.assertQueue(queue);

  console.log("Đang chờ nhận thông báo...");

  channel.consume(queue, (msg) => {
    const data = JSON.parse(msg.content.toString());

    console.log("Thông báo:", data.message);

    channel.ack(msg);
  });
}

receiveMessage();