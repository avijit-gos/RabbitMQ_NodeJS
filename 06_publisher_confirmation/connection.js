/** @format */

const amqp = require("amqplib");

async function createConnection(url, retries = 3) {
  try {
    const connection = await amqp.connect(url);
    const channel = await connection.createConfirmChannel();

    channel.on("error", () => console.log("Error in RabbitMQ connection"));
    channel.on("close", () => {
      console.log("RabbitMQ closed & ready to retry");
      setTimeout(() => createConnection(url, retries), 3000);
    });

    return channel;
  } catch (error) {
    console.log("ERROR:", error?.message);
    if (retries <= 0) throw new Error("All 3 retries accumulated");
    return new Promise((resolve) => {
      setTimeout(() => resolve(createConnection(url, retries - 1), 3000));
    });
  }
}

module.exports = createConnection;
