/** @format */

const createConnection = require("./connection");

const CONFIGS = {
  CONNECTION_URL: "amqp://localhost",
  EXCHANGE_NAME: "app_exchange_7",
  EXCHANGE_TYPE: "direct",
  QUEUE_NAME: "queue_7",
  ROUTING_KEY: "key_7",
};

const messages = [
  { _id: 1, message: "This is message 1" },
  { _id: 2, message: "This is message 2" },
  { _id: 3, message: "This is message 3" },
  { _id: 4, message: "This is message 4" },
  { _id: 5, message: "This is message 5" },
  { _id: 6, message: "This is message 6" },
  { _id: 7, message: "This is message 7" },
  { _id: 8, message: "This is message 8" },
  { _id: 9, message: "This is message 9" },
  { _id: 10, message: "This is message 10" },
  { _id: 11, message: "This is message 11" },
  { _id: 12, message: "This is message 12" },
  { _id: 13, message: "This is message 13" },
  { _id: 14, message: "This is message 14" },
  { _id: 15, message: "This is message 15" },
  { _id: 16, message: "This is message 16" },
  { _id: 17, message: "This is message 17" },
  { _id: 18, message: "This is message 18" },
  { _id: 19, message: "This is message 19" },
  { _id: 20, message: "This is message 20" },
  { _id: 21, message: "This is message 21" },
];

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

    for (let msg of messages) {
      let response = await channel.publish(
        CONFIGS.EXCHANGE_NAME,
        CONFIGS.ROUTING_KEY,
        Buffer.from(JSON.stringify(msg)),
        { persistance: true }
      );

      if (!response) {
        console.log(
          "Could not publish more messages & waiting for to drain queue"
        );
        channel.once("drain", () => {
          console.log("Draining queue...");
        });
      }
      console.log(`Message with ID:${msg._id} successfully published...`);
    }
    console.log("All messages published successfully");
  } catch (error) {
    console.log("ERROR:", error?.message);
  }
}
producer();
