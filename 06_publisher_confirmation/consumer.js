/** @format */

const amqp = require("amqplib");
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

async function consumer() {
  try {
    // Establish rabbitMQ connection
    const connection = await amqp.connect(CONFIGS.CONNECTION_URL);
    // Create a channel
    const channel = await connection.createChannel();

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

    channel.prefetch(5);

    channel.consume(CONFIGS.QUEUE_NAME, (message) => {
      if (message) {
        const data = JSON.parse(message?.content);
        console.log("Message successfully consumed", data);
        channel.ack(message);
      }
    });
  } catch (error) {
    console.log("ERROR:", error?.consumer);
  }
}
consumer();
