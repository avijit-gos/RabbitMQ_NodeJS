/** @format */

const amqp = require("amqplib");
const CONFIGS = {
  CONNECTION_URL: "amqp://localhost",
  EXCHANGE_NAME: "app_exchange_4",
  EXCHANGE_TYPE: "topic",
  ROUTING_PATTERN: "user.active.IN",
};

async function producer() {
  try {
    const connection = await amqp.connect(CONFIGS.CONNECTION_URL);
    const channel = await connection.createChannel();

    await channel.assertExchange(CONFIGS.EXCHANGE_NAME, CONFIGS.EXCHANGE_TYPE, {
      durable: true,
    });

    channel.publish(
      CONFIGS.EXCHANGE_NAME,
      CONFIGS.ROUTING_PATTERN,
      Buffer.from(JSON.stringify(require("../../../mock_data/user.json")))
    );

    console.log("Producer successfully publish message...");
  } catch (error) {
    console.log("ERROR:", error?.message);
  }
}
producer();
