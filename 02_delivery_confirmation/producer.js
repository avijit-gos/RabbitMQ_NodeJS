/** @format */
const createConnection = require("./connection.js");

const CONFIGS = {
  CONNECTION_URL: "amqp://localhost",
  EXCHANGE_NAME: "app_exchange_5",
  EXCHANGE_TYPE: "direct",
  QUEUE_NAME: "queue_5",
  ROUTING_KEY: "key_5",
};

async function producer() {
  try {
    const channel = await createConnection(CONFIGS.CONNECTION_URL);

    await channel.assertExchange(CONFIGS.EXCHANGE_NAME, CONFIGS.EXCHANGE_TYPE, {
      durable: true,
    });
    await channel.assertQueue(CONFIGS.QUEUE_NAME, { durable: true });

    await channel.bindQueue(
      CONFIGS.QUEUE_NAME,
      CONFIGS.EXCHANGE_NAME,
      CONFIGS.ROUTING_KEY
    );

    channel.publish(
      CONFIGS.EXCHANGE_NAME,
      CONFIGS.ROUTING_KEY,
      Buffer.from(JSON.stringify(require("../mock_data/user.json"))),
      { persistent: true },
      (err, ok) => {
        if (err)
          console.log("Could not publish message from producer,", err.message);
        console.log("Message successfully publish message");
      }
    );
  } catch (error) {
    console.log("ERROR:", error?.message);
  }
}
producer();
