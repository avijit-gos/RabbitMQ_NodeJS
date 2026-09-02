/** @format */

const amqp = require("amqplib");
const CONFIGS = {
  CONNECTION_URL: "amqp://localhost",
  EXCHANGE_NAME: "app_exchange_2",
  EXCHANGE_TYPE: "direct",
  QUEUE_DETAILS: [
    {
      TYPE: "user",
      QUEUE_NAME: "user_queue",
      ROUTING_KEY: "user",
    },
    {
      TYPE: "product",
      QUEUE_NAME: "product_queue",
      ROUTING_KEY: "product",
    },
  ],
};
const messages = [
  {
    type: "user",
    data: require("../../../mock_data/user.json"),
  },
  {
    type: "product",
    data: require("../../../mock_data/product.json"),
  },
];

async function producer() {
  try {
    const connection = await amqp.connect(CONFIGS.CONNECTION_URL);
    const channel = await connection.createChannel();

    await channel.assertExchange(CONFIGS.EXCHANGE_NAME, CONFIGS.EXCHANGE_TYPE, {
      durable: true,
    });

    for (let config of CONFIGS.QUEUE_DETAILS) {
      await channel.assertQueue(config.QUEUE_NAME, { durable: true });
      await channel.bindQueue(
        config.QUEUE_NAME,
        CONFIGS.EXCHANGE_NAME,
        config.ROUTING_KEY
      );
    }

    for (let msg of messages) {
      let config = CONFIGS.QUEUE_DETAILS.find(
        (config) => config.TYPE === msg.type
      );
      channel.publish(
        CONFIGS.EXCHANGE_NAME,
        config.ROUTING_KEY,
        Buffer.from(JSON.stringify(msg.data))
      );
      console.log(
        `Producer suceessfully produced ${config.TYPE.toUpperCase()}`
      );
    }
  } catch (error) {
    console.log("ERROR:", error?.message);
  }
}
producer();
