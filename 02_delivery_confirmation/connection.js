/** @format */

const amqp = require("amqplib");

async function createConnection(url, retries = 3) {
  try {
    if (!url) {
      console.log("Invalid connection url");
      return;
    }
    const connection = await amqp.connect(url);
    const channel = await connection.createConfirmChannel();

    channel.on("error", (err) =>
      console.log("RabbitMQ connection error, ", err.message)
    );
    channel.on("close", () => {
      console.log("Channel closed & try to reconnect");
      setTimeout(() => createConnection(url, retries), 2000);
    });

    return channel;
  } catch (error) {
    if (retries <= 0) throw error;
    console.log("ERROR:", error?.message);
    return new Promise((resolve) => {
      setTimeout(() => resolve(createConnection(url, retries - 1)), 3000);
    });
  }
}

module.exports = createConnection;
