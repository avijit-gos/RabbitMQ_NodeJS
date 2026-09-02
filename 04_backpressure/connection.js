/** @format */

const amqp = require("amqplib");

async function createConnection(url, retries = 3) {
  try {
    const connection = await amqp.connect(url);
    const channel = await connection.createConfirmChannel();

    channel.on("error", () => console.log("ERROR: RabbitMQ connection error"));
    channel.on(
      "close",
      () => setTimeout(() => createConnection(url, retries)),
      3000
    );

    return channel;
  } catch (error) {
    if (retries <= 0)
      throw new Error(`All ${retries} retries attemps, but connection failed`);

    return new Promise((resolve) =>
      setTimeout(resolve(createConnection(url, retries - 1)), 3000)
    );
  }
}

module.exports = createConnection;
