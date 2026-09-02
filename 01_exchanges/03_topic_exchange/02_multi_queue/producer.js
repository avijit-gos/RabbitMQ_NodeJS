/** @format */

const amqp = require("amqplib");
const CONFIGS = {
  CONNECTION_URL: "amqp://localhost",
  EXCHANGE_NAME: "app_exchange_4",
  EXCHANGE_TYPE: "topic",
  QUEUE_DETAILS: [
    { TYPE: "user", ROUTING_KEY: "ative.user.in" },
    { TYPE: "product", ROUTING_KEY: "purchase.product" },
    { TYPE: "notification", ROUTING_KEY: "notification.payment.success" },
  ],
};
const messages = [
  { TYPE: "user", DATA: require("../../../mock_data/user.json") },
  { TYPE: "product", ROUTING_KEY: require("../../../mock_data/product.json") },
  {
    TYPE: "notification",
    ROUTING_KEY: {
      _id: 1,
      amount: 100,
      message: "Payment successfull, order proceed for next step.",
    },
  },
];

async function producer() {
  try {
    const connection = await amqp.connect(CONFIGS.CONNECTION_URL);
    const channel = await connection.createChannel();

    await channel.assertExchange(CONFIGS.EXCHANGE_NAME, CONFIGS.EXCHANGE_TYPE, {
      durable: true,
    });

    for (let msg of messages) {
      const config = CONFIGS.QUEUE_DETAILS.find((con) => con.TYPE === msg.TYPE);
      channel.publish(
        CONFIGS.EXCHANGE_NAME,
        config.ROUTING_KEY,
        Buffer.from(JSON.stringify(msg))
      );
      console.log(
        `${config.TYPE.toUpperCase()} message successfully published`
      );
    }
  } catch (error) {
    console.log("ERROR:", error.message);
  }
}
producer();
