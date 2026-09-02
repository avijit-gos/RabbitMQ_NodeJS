/** @format */

const amqp = require("amqplib");
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

async function consumer() {
  try {
    const connection = await amqp.connect(CONFIGS.CONNECTION_URL);
    const channel = await connection.createChannel();

    // Assert main EXCHNAGE
    await channel.assertExchange(CONFIGS.EXCHANGE_NAME, CONFIGS.EXCHANGE_TYPE, {
      durable: true,
    });
    // Assert dead letter exchange
    await channel.assertExchange(
      CONFIGS.DEAD_LETTER_EXCHANGE,
      CONFIGS.DEAD_LETTER_EXCHANGE_TYPE,
      { durable: true }
    );
    // Assert dead letter queue
    await channel.assertQueue(CONFIGS.DEAD_LETTER_QUEUE, { durable: true });

    // Bind dead letter queue
    await channel.bindQueue(
      CONFIGS.DEAD_LETTER_QUEUE,
      CONFIGS.DEAD_LETTER_EXCHANGE,
      CONFIGS.DEAD_LETTER_ROUTING_KEY
    );

    // Assert main QUEUE
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
        const user = JSON.parse(message?.content);
        if (user.id % 2 === 0) {
          console.log("✅ SUCCESS");
          console.log(user);
          channel.ack(message);
        } else {
          console.log("❌ ERROR");
          channel.nack(message, false, false);
        }
      }
    });
  } catch (error) {
    console.log("ERROR:", error?.message);
  }
}
consumer();
