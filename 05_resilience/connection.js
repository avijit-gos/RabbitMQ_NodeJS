/** @format */

const amqp = require("amqplib");

async function createConnection(url, retries = 3) {
  try {
    const connection = await amqp.connect(url);
    const channel = await connection.createConfirmChannel();

    channel.on("error", () => console.log("RabbitMQ connection error..."));
    channel.on("close", () => {
      console.log("RabbitMq connection closed & ready to retry");
      setTimeout(() => createConnection(url, retries), 3000);
    });

    return channel;
  } catch (error) {
    console.log("Error:", error?.message);
    return new Promise((resolve) => {
      setTimeout(() => resolve(createConnection(url, retries - 1)), 3000);
    });
  }
}

module.exports = createConnection;
