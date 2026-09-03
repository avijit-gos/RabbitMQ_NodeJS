/** @format */

const amqp = require("amqplib");

async function createConnection(url, retries = 3) {
  try {
    const connection = await amqp.connect(url);

    let isBlocked = false;

    connection.on("error", (error) =>
      console.log("RabbitMQ connection error,", error?.message)
    );
    connection.on("close", () => {
      console.log("RabbitMq connection closed and retry again");
      setTimeout(() => {
        createConnection(url, retries);
      }, 3000);
    });

    connection.on("blocked", () => {
      console.log("RabbitMQ connection blocked...");
      isBlocked = true;
    });
    connection.on("unblocked", () => {
      console.log("RabbitMQ connection unblocked & ready to reconnect");
      isBlocked = false;
    });

    /**
     * সরাসরি isBlocked রিটার্ন করলে সমস্যা কী হতো?
     * তাহলে ঠিক ওই মুহূর্তে isBlocked-এর যা মান (তখন false), সেটার একটা কপি রিটার্ন হতো। এটা একটা স্ন্যাপশটের মতো, যেটা সময়ের সাথে
     * আর বদলাবে না। পরে যখন 'blocked' ইভেন্ট ঘটবে আর isBlocked = true হবে, তখনও যে এই রিটার্ন করা object ধরে রেখেছে সে
     * পুরনো false-ই দেখবে — কারণ JavaScript-এ boolean, number, string এর মতো primitive টাইপ value দিয়ে কপি হয়,
     * reference দিয়ে না।
     *
     * ফাংশন ব্যবহার করলে কেন কাজ করে?
     * এটা একটা arrow function, যেটা তার বাইরের স্কোপের isBlocked ভ্যারিয়েবলটাকে closure-এর মাধ্যমে ধরে রাখে — মানে এটা ভ্যারিয়েবলের
     * একটা "লাইভ" রেফারেন্স রাখে, কোনো ফিক্সড কপি না। প্রতিবার এই ফাংশন isBlockedRef() কল করলে, সেটা সেই মুহূর্তের বর্তমান মান দেখে
     * আনে, ফাংশন তৈরি হওয়ার সময়কার পুরনো মান না।
     */
    return { connection, isBlockedRef: () => isBlocked };
  } catch (error) {
    if (retries <= 0) throw new Error("All retries attempt");
    return new Promise((resolve) => {
      setTimeout(() => resolve(createConnection(url, retries - 1)), 3000);
    });
  }
}

module.exports = createConnection;
