const { startConsumer } = require("./kafka/consumer");

async function start() {

  await startConsumer();

  console.log("Inventory service started");

}

start();