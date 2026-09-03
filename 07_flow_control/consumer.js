/** @format */

const amqp = require("amqplib");
const createConnection = require("./connection");
const CONFIGS = {
  CONNECTION_URL: "amqp://localhost",
  EXCHANGE_NAME: "app_exchange_10",
  EXCHANGE_TYPE: "direct",
  QUEUE_NAME: "queue_10",
  ROUTING_KEY: "key_10",
  DEAD_LETTER_EXCHANGE: "dead_exchange_3",
  DEAD_LETTER_EXCHANGE_TYPE: "direct",
  DEAD_LETTER_QUEUE: "dead_queue_3",
  DEAD_LETTER_ROUTING_KEY: "dead-key_3",
};

async function consumer() {
  try {
    const { connection, isBlockedRef } = await createConnection(
      CONFIGS.CONNECTION_URL
    );

    while (isBlockedRef()) {
      console.log("RabbitMQ broker blocked");
      await new Promise((resolve) => setTimeout(resolve, 500));
    }

    const channel = await connection.createChannel();
    channel.assertExchange(CONFIGS.EXCHANGE_NAME, CONFIGS.EXCHANGE_TYPE, {
      durable: true,
    });
    await channel.assertExchange(
      CONFIGS.DEAD_LETTER_EXCHANGE,
      CONFIGS.DEAD_LETTER_EXCHANGE_TYPE,
      { durable: true }
    );

    await channel.assertQueue(CONFIGS.DEAD_LETTER_QUEUE, { durable: true });
    await channel.bindQueue(
      CONFIGS.DEAD_LETTER_QUEUE,
      CONFIGS.DEAD_LETTER_EXCHANGE,
      CONFIGS.DEAD_LETTER_ROUTING_KEY
    );

    await channel.assertQueue(CONFIGS.QUEUE_NAME, {
      durable: true,
      arguments: {
        "x-dead-letter-exchange": CONFIGS.DEAD_LETTER_EXCHANGE,
        "x-dead-letter-routing-key": CONFIGS.DEAD_LETTER_ROUTING_KEY,
      },
    });
    await channel.bindQueue(
      CONFIGS.QUEUE_NAME,
      CONFIGS.EXCHANGE_NAME,
      CONFIGS.ROUTING_KEY
    );

    channel.prefetch(5);

    channel.consume(CONFIGS.QUEUE_NAME, (message) => {
      try {
        if (message) {
          const data = JSON.parse(message?.content);
          console.log("📥 RECEIVED:", data.id);

          setTimeout(() => {
            console.log("✅ ACK:", data.id);
            channel.ack(message);
          }, 3000);
        }
      } catch (error) {
        channel.nack(message, false, false);
      }
    });
  } catch (error) {
    console.log("ERROR:", error);
  }
}
consumer();
