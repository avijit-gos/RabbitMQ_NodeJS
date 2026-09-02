/** @format */

const createConnection = require("./connection");

const CONFIGS = {
  CONNECTION_URL: "amqp://localhost",
  EXCHANGE_NAME: "app_exchange_9",
  EXCHANGE_TYPE: "direct",
  QUEUE_NAME: "queue_9",
  ROUTING_KEY: "key_9",
  DEAD_LETTER_EXCHANGE: "dead_exchange_2",
  DEAD_LETTER_EXCHANGE_TYPE: "direct",
  DEAD_LETTER_QUEUE: "dead_queue_2",
  DEAD_LETTER_ROUTING_KEY: "dead-key_2",
};

async function producer() {
  try {
    const channel = await createConnection(CONFIGS.CONNECTION_URL);

    // Assert main exchange
    await channel.assertExchange(CONFIGS.EXCHANGE_NAME, CONFIGS.EXCHANGE_TYPE, {
      durable: true,
    });
    // Assert dead-letter exchange
    await channel.assertExchange(
      CONFIGS.DEAD_LETTER_EXCHANGE,
      CONFIGS.DEAD_LETTER_EXCHANGE_TYPE,
      { durable: true }
    );

    // Assert dead-letter queue
    await channel.assertQueue(CONFIGS.DEAD_LETTER_QUEUE, { durable: true });
    // Bind dead-letter queue with exchange using routing key
    await channel.bindQueue(
      CONFIGS.DEAD_LETTER_QUEUE,
      CONFIGS.DEAD_LETTER_EXCHANGE,
      CONFIGS.DEAD_LETTER_ROUTING_KEY
    );

    // Assert main queue
    await channel.assertQueue(CONFIGS.QUEUE_NAME, {
      durable: true,
      arguments: {
        "x-dead-letter-exchange": CONFIGS.DEAD_LETTER_EXCHANGE,
        "x-dead-letter-routing-key": CONFIGS.DEAD_LETTER_ROUTING_KEY,
      },
    });
    // Bind main queue with exchange using routing key
    await channel.bindQueue(
      CONFIGS.QUEUE_NAME,
      CONFIGS.EXCHANGE_NAME,
      CONFIGS.ROUTING_KEY
    );

    // Publish message
    const ok = channel.publish(
      CONFIGS.EXCHANGE_NAME,
      CONFIGS.ROUTING_KEY,
      Buffer.from(JSON.stringify(require("../mock_data/user.json"))),
      { persistence: true }
    );
    if (!ok) {
      console.log("Could not publish more messages");
      channel.once("drain", () => console.log("Drain messages from queue"));
    }
    console.log("Message successfully published");
  } catch (error) {
    console.log("ERROR:", error?.message);
  }
}
producer();
