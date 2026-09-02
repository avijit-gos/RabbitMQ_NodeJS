/** @format */

const createConnection = require("./connection");

const CONFIGS = {
  CONNECTION_URL: "amqp://localhost",
  EXCHANGE_NAME: "app_exchange_8",
  EXCHANGE_TYPE: "direct",
  QUEUE_NAME: "queue_8",
  ROUTING_KEY: "key_8",
  DEAD_LETTER_EXCHANGE: "dead_exchange_1",
  DEAD_LETTER_EXCHANGE_TYPE: "direct",
  DEAD_LETTER_QUEUE: "dead_queue_1",
  DEAD_LETTER_ROUTING_KEY: "dead-key_1",
};

async function producer() {
  try {
    const channel = await createConnection(CONFIGS.CONNECTION_URL);

    // MAIN EXCHANGE
    await channel.assertExchange(CONFIGS.EXCHANGE_NAME, CONFIGS.EXCHANGE_TYPE, {
      durable: true,
    });
    // DEAD LETTER EXCHANGE
    await channel.assertExchange(
      CONFIGS.DEAD_LETTER_EXCHANGE,
      CONFIGS.DEAD_LETTER_EXCHANGE_TYPE,
      {
        durable: true,
      }
    );

    // DEAD LETTER QUEUE
    await channel.assertQueue(CONFIGS.DEAD_LETTER_QUEUE, { durable: true });

    // BIND DEAD LETTER QUEUE WITH EXCHANGE
    await channel.bindQueue(
      CONFIGS.DEAD_LETTER_QUEUE,
      CONFIGS.DEAD_LETTER_EXCHANGE,
      CONFIGS.DEAD_LETTER_ROUTING_KEY
    );

    // ASSERT MAIN QUEUE
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

    const res = await channel.publish(
      CONFIGS.EXCHANGE_NAME,
      CONFIGS.ROUTING_KEY,
      Buffer.from(JSON.stringify(require("../mock_data/user.json"))),
      { persistent: true }
    );

    if (!res) {
      console.log("Could not publish more message to queue");
      channel.once("drain", console.log("Queue draining..."));
    }
    console.log("All message successfully published...");
  } catch (error) {
    console.log("ERROR:", error?.message);
  }
}
producer();
