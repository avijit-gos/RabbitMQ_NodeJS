/** @format */

const amqp = require("amqplib");
const User = require("../../../mock_data/user.json");
const CONFIGS = {
  CONNECTION_URL: "amqp://localhost",
  EXCHANGE_NAME: "app_exchange_1",
  EXCHANGE_TYPE: "direct",
  QUEUE_NAME: "app_queue_1",
  ROUTING_KEY: "key_1",
};

async function producer() {
  try {
    const connection = await amqp.connect(CONFIGS?.CONNECTION_URL);
    const channel = await connection.createChannel();

    await channel.assertExchange(
      CONFIGS?.EXCHANGE_NAME,
      CONFIGS?.EXCHANGE_TYPE,
      { durable: true }
    );
    await channel.assertQueue(CONFIGS.QUEUE_NAME, { durable: true });

    await channel.bindQueue(
      CONFIGS.QUEUE_NAME,
      CONFIGS.EXCHANGE_NAME,
      CONFIGS.ROUTING_KEY
    );

    channel.publish(
      CONFIGS.EXCHANGE_NAME,
      CONFIGS.ROUTING_KEY,
      Buffer.from(JSON.stringify(User)),
      { persistent: true }
    );
    console.log("Producer successfully publish message...");
  } catch (error) {
    console.log("ERROR:", error?.message);
  }
}
producer();
