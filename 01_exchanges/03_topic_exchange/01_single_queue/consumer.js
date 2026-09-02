/** @format */

const amqp = require("amqplib");
const CONFIGS = {
  CONNECTION_URL: "amqp://localhost",
  EXCHANGE_NAME: "app_exchange_4",
  EXCHANGE_TYPE: "topic",
  ROUTING_KEY: "user.#",
};

async function consumer() {
  try {
    const connection = await amqp.connect(CONFIGS.CONNECTION_URL);
    const channel = await connection.createChannel();

    await channel.assertExchange(CONFIGS.EXCHANGE_NAME, CONFIGS.EXCHANGE_TYPE, {
      durable: true,
    });
    const queue = await channel.assertQueue("", {
      exclusive: true,
      autoDelete: true,
    });

    await channel.bindQueue(
      queue.queue,
      CONFIGS.EXCHANGE_NAME,
      CONFIGS.ROUTING_KEY
    );

    channel.consume(queue.queue, (message) => {
      if (message) {
        const data = JSON.parse(message.content);
        console.log("Consumer successfully consume message...");
        console.log(data);
        channel.ack(message);
      }
    });
  } catch (error) {
    console.log("ERROR:", error?.message);
  }
}
consumer();
