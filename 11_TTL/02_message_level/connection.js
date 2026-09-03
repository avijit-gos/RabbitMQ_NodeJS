/** @format */

const amqp = require("amqplib");

async function createConnection(url, retries = 3) {
  try {
    const connection = await amqp.connect(url);
    let isBlocked = false;

    connection.on("error", () => console.log("RabbitMQ connection error"));
    connection.on("close", () => {
      console.log("RabbitMq connection closed & ready to retry");
      setTimeout(() => createConnection(url, retries), 3000);
    });

    connection.on("blocked", () => {
      console.log("RabbitMQ connection blocked");
      isBlocked = true;
    });
    connection.on("unblocked", () => {
      console.log("RabbitMQ connection unblocked");
      isBlocked = false;
    });
    return { connection, isBlockedRef: () => isBlocked };
  } catch (error) {
    console.log("ERROR:", error?.message);
    if (retries <= 0) throw new Error("All retries attempt");
    return new Promise((resolve) =>
      setTimeout(() => {
        resolve(createConnection(url, retries - 1));
      }, 3000)
    );
  }
}

module.exports = createConnection;
