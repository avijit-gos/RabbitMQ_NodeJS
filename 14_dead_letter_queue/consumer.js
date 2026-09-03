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

async function consumer() {
  try {
    const { connection, isBlockedRef } = await createConnection(
      CONFIGS.CONNECTION_URL
    );
    while (isBlockedRef()) {
      console.log("RabbitMQ broker blocked");
      await new Promise((resolve) => setTimeout(resolve, 3000));
    }
    const channel = await connection.createChannel();

    await channel.assertExchange(CONFIGS.EXCHANGE_NAME, CONFIGS.EXCHANGE_TYPE, {
      durable: true,
    });
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
          const data = JSON.parse(message.content);
          if (data._id % 2 === 0) {
            console.log("Message successfully consumed", data);
            channel.ack(message);
          } else {
            console.log("Message could not consumed & goes to DLQ");
            channel.nack(message, false, false);
          }
        }
      } catch (error) {
        channel.nack(message, false, false);
      }
    });
  } catch (error) {
    console.log("ERROR:", error?.message);
  }
}
consumer();
