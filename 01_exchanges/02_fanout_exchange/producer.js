/** @format */

const amqp = require("amqplib");
const CONFIGS = {
  CONNECTION_URL: "amqp://localhost",
  EXCHANGE_NAME: "app_exchange_3",
  EXCHANGE_TYPE: "fanout",
};
const message = {
  _id: 1,
  message: "User successfully placed order.",
  type: "order_notification",
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
      "",
      Buffer.from(JSON.stringify(message))
    );
    console.log("Producer successfully published message...");
  } catch (error) {
    console.log("ERROR:", error?.message);
  }
}
producer();
