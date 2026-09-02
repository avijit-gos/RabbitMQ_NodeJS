/** @format */

const amqp = require("amqplib");
const CONFIGS = {
  CONNECTION_URL: "amqp://localhost",
  EXCHANGE_NAME: "app_exchange_6",
  EXCHANGE_TYPE: "direct",
  QUEUE_NAME: "queue_6",
  ROUTING_KEY: "key_6",
};

async function consumer() {
  try {
    const connection = await amqp.connect(CONFIGS.CONNECTION_URL);
    const channel = await connection.createChannel();

    await channel.assertExchange(CONFIGS.EXCHANGE_NAME, CONFIGS.EXCHANGE_TYPE, {
      durable: true,
    });
    await channel.assertQueue(CONFIGS.QUEUE_NAME, { durable: true });

    await channel.bindQueue(
      CONFIGS.QUEUE_NAME,
      CONFIGS.EXCHANGE_NAME,
      CONFIGS.ROUTING_KEY
    );

    channel.consume(CONFIGS.QUEUE_NAME, (message) => {
      if (message) {
        const data = JSON.parse(message?.content);
        console.log("Consumer successfully consume message...");
        console.log(data);
        channel.ack(message);
      } else {
        channel.nack(message, false, false);
      }
    });
  } catch (error) {
    console.log("ERROR:", error?.message);
  }
}
consumer();
