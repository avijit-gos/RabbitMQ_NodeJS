/** @format */

const createConnection = require("./connection");

const CONFIGS = {
  CONNECTION_URL: "amqp://localhost",
  EXCHANGE_NAME: "app_exchange_12",
  EXCHANGE_TYPE: "direct",
  QUEUE_NAME: "queue_12",
  ROUTING_KEY: "key_12",
  DEAD_LETTER_EXCHANGE: "dead_exchange_5",
  DEAD_LETTER_EXCHANGE_TYPE: "direct",
  DEAD_LETTER_QUEUE: "dead_queue_5",
  DEAD_LETTER_ROUTING_KEY: "dead-key_5",
};

const messages = [
  { _id: 1, message: "This is sample message 1" },
  { _id: 2, message: "This is sample message 2" },
  { _id: 3, message: "This is sample message 3" },
];

async function producer() {
  try {
    const { connection, isBlockedRef } = await createConnection(
      CONFIGS.CONNECTION_URL
    );

    while (isBlockedRef()) {
      console.log("RabbitMQ broker blocked");
      await new Promise((resolve) => setTimeout(resolve, 500));
    }

    const channel = await connection.createConfirmChannel();

    await channel.assertExchange(CONFIGS.EXCHANGE_NAME, CONFIGS.EXCHANGE_TYPE, {
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

    for (let msg of messages) {
      const ok = channel.publish(
        CONFIGS.EXCHANGE_NAME,
        CONFIGS.ROUTING_KEY,
        Buffer.from(JSON.stringify(msg)),
        { persistent: true, expiration: msg._id * 3000 }
      );

      if (!ok) {
        console.log("Backpressure");
        await new Promise((resolve) => {
          channel.once("drain", () => {
            console.log("DRAIN event trigger");
            resolve();
          });
        });
      }
      console.log("All message published");
    }
  } catch (error) {
    console.log("ERROR:", error);
  }
}
producer();
