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
      await new Promise((resolve) => setTimeout(resolve, 3000));
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
      let ok = channel.publish(
        CONFIGS.EXCHANGE_NAME,
        CONFIGS.ROUTING_KEY,
        Buffer.from(JSON.stringify(msg)),
        { persistent: true }
      );

      if (!ok) {
        console.log("Backpressure hit");

        await new Promise((resolve) => {
          channel.once("drain", () => {
            console.log("DRAIN event trigger");
            resolve();
          });
        });
      }
      console.log("All message successfully published");
    }
  } catch (error) {
    console.log("ERROR:", error?.message);
  }
}
producer();
