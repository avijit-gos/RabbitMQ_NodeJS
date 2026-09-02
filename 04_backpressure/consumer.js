/** @format */

const amqp = require("amqplib");
const CONFIGS = {
  CONNECTION_URL: "amqp://localhost",
  EXCHANGE_NAME: "app_exchange_7",
  EXCHANGE_TYPE: "direct",
  QUEUE_NAME: "queue_7",
  ROUTING_KEY: "key_7",
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
      CONFIGS.EXCHANGE_TYPE
    );

    channel.prefetch(5);

    channel.consume(CONFIGS.QUEUE_NAME, async (message) => {
      try {
        const data = JSON.parse(message.content);
        console.log(data.message);
        setTimeout(() => {
          console.log("\nWaiting for 3s to load more message\n");
          channel.ack(message);
        }, 3000);
      } catch (error) {
        console.log("ERROR:", error?.message);
        channel.nack(message, false, false);
      }
    });
  } catch (error) {
    console.log("ERROR:", error?.message);
  }
}

consumer();
