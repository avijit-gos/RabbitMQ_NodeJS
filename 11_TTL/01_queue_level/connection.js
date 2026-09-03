/** @format */

const amqp = require("amqplib");

async function createConnection(url, retries = 3) {
  try {
    const connection = await amqp.connect(url);
    let isBlocked = false;

    connection.on("error", (err) =>
      console.log("RabbitMQ connection error,", err?.message)
    );
    connection.on("close", () => {
      console.log("RabbitMq connection closed and ready to retry");
      setTimeout(() => createConnection(url, retries), 3000);
    });

    connection.on("blocked", (reason) => {
      console.log("RabbitMQ connection is blocked", reason.reason);
      isBlocked = true;
    });
    connection.on("unblocked", () => {
      console.log("RabbitMQ connection is unblocked");
      isBlocked = false;
    });

    return { connection, isBlockedRef: () => isBlocked };
  } catch (error) {
    console.log("ERROR:", error?.message);
    if (retries <= 0) throw new Error("All retries attemp occured");
    return new Promise((resolve) =>
      setTimeout(() => resolve(createConnection(url, retries - 1)), 3000)
    );
  }
}

module.exports = createConnection;
