/** @format */

const createConnection = require("./connection");

const CONFIGS = {
  CONNECTION_URL: "amqp://localhost",
  EXCHANGE_NAME: "app_exchange_13",
  EXCHANGE_TYPE: "direct",
  QUEUE_NAME: "queue_13",
  ROUTING_KEY: "key_13",
  DEAD_LETTER_EXCHANGE: "dead_exchange_6",
  DEAD_LETTER_EXCHANGE_TYPE: "direct",
  DEAD_LETTER_QUEUE: "dead_queue_6",
  DEAD_LETTER_ROUTING_KEY: "dead-key_6",
};

async function deadLetterConsumer() {
  try {
    const { connection, isBlockedRef } = await createConnection(
      CONFIGS.CONNECTION_URL
    );

    while (isBlockedRef()) {
      console.log("RabbitMQ broker blocked");
      await new Promise((resolve) => setTimeout(resolve, 3000));
    }
    const channel = await connection.createChannel();
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

    channel.prefetch(5);

    channel.consume(CONFIGS.DEAD_LETTER_QUEUE, (message) => {
      if (message) {
        const data = JSON.parse(message.content);
        console.log("Message successfully consumed in DLQ,", data);
      }
    });
  } catch (error) {
    console.log(error);
  }
}
deadLetterConsumer();
