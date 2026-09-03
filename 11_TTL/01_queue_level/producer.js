/** @format */

const createConnection = require("./connection");

const CONFIGS = {
  CONNECTION_URL: "amqp://localhost",
  EXCHANGE_NAME: "app_exchange_11",
  EXCHANGE_TYPE: "direct",
  QUEUE_NAME: "queue_11",
  ROUTING_KEY: "key_11",
  DEAD_LETTER_EXCHANGE: "dead_exchange_4",
  DEAD_LETTER_EXCHANGE_TYPE: "direct",
  DEAD_LETTER_QUEUE: "dead_queue_4",
  DEAD_LETTER_ROUTING_KEY: "dead-key_4",
};

async function producer() {
  try {
    const { connection, isBlockedRef } = await createConnection(
      CONFIGS.CONNECTION_URL
    );
    while (isBlockedRef()) {
      console.log("Broker currently blocked...");
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

    // Queue level TTL
    await channel.assertQueue(CONFIGS.QUEUE_NAME, {
      durable: true,
      arguments: {
        "x-dead-letter-exchange": CONFIGS.DEAD_LETTER_EXCHANGE,
        "x-dead-letter-routing-key": CONFIGS.DEAD_LETTER_ROUTING_KEY,
        "x-message-ttl": 5000,
      },
    });
    await channel.bindQueue(
      CONFIGS.QUEUE_NAME,
      CONFIGS.EXCHANGE_NAME,
      CONFIGS.ROUTING_KEY
    );

    const ok = channel.publish(
      CONFIGS.EXCHANGE_NAME,
      CONFIGS.ROUTING_KEY,
      Buffer.from(JSON.stringify(require("../../mock_data/product.json"))),
      { persistent: true }
    );

    if (!ok) {
      console.log("Backpressure....");
      await new Promise((resolve) => {
        channel.once("drain", () => {
          console.log("Drain event hit...");
          resolve();
        });
      });
    }
    console.log("Message publish successfully");
  } catch (error) {
    console.log("ERROR:", error?.message);
  }
}

producer();
