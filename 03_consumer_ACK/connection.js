/** @format */

const amqp = require("amqplib");

async function createConnection(url, retry = 3) {
  try {
    if (!url) {
      console.log("Invalid RabbitMQ connection URL");
      return;
    }
    const connection = await amqp.connect(url);
    const channel = await connection.createConfirmChannel();

    channel.on("error", () => console.log("RabbitMq connection error"));
    channel.on("close", () => {
      console.log("RabbitMQ connection closed & retry for new connection");
      setTimeout(() => createConnection(url, retry), 3000);
    });

    return channel;
  } catch (error) {
    if (retry <= 0) throw new Error(`All ${retry} retries called...`);

    return new Promise((resolve) => {
      setTimeout(() => resolve(createConnection(url, retries - 1)), 3000);
    });

    console.log(error?.message);
  }
}
module.exports = createConnection;
