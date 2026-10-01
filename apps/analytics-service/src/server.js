const { startConsumer } = require("./kafka/consumer");

async function start() {
  const stopConsumer = await startConsumer();

  console.log("Analytics service started");

  const shutdown = async (signal) => {
    console.log(`${signal} received; stopping analytics consumer`);

    try {
      await stopConsumer();
      process.exit(0);
    } catch (error) {
      console.error("Failed to stop analytics consumer", error);
      process.exit(1);
    }
  };

  process.once("SIGINT", () => shutdown("SIGINT"));
  process.once("SIGTERM", () => shutdown("SIGTERM"));
}

start().catch((error) => {
  console.error("Unable to start analytics service", error);
  process.exitCode = 1;
});
