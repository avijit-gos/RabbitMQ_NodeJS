/** @format */

const amqp = require("amqplib");
const CONFIGS = {
  CONNECTION_URL: "amqp://localhost",
  EXCHANGE_NAME: "app_exchange_4",
  EXCHANGE_TYPE: "topic",
  ROUTING_PATTERN: "notification.#",
};

async function notificationConsumer() {
  try {
    const connection = await amqp.connect(CONFIGS.CONNECTION_URL);
    const channel = await connection.createChannel();

    await channel.assertExchange(CONFIGS.EXCHANGE_NAME, CONFIGS.EXCHANGE_TYPE, {
      durable: true,
    });
    const queue = await channel.assertQueue("", {
      autoDelete: true,
      exclusive: true,
    });

    await channel.bindQueue(
      queue?.queue,
      CONFIGS.EXCHANGE_NAME,
      CONFIGS.ROUTING_PATTERN
    );

    channel.consume(queue?.queue, (message) => {
      if (message) {
        const data = JSON.parse(message?.content);
        console.log("NOTIFICATION consumer successfully consume message");
        console.log(data);
        channel.ack(message);
      }
    });
  } catch (error) {
    console.log("ERROR:", error?.message);
  }
}
notificationConsumer();
