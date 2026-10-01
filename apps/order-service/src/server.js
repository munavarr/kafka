const express = require("express");

const {
  connectProducer,
  publishOrderCreated,
} = require("./kafka/producer");
const {
  isValidProductId,
} = require("../../../shared/kafka/message-keys");

const app = express();

app.use(express.json());

app.get('/api/order', (req, res) => {
  res.json({
    service: 'order-service',
    order: [
      {
        id: 1,
        name: 'iPhone',
        price: 999,
      },
      {
        id: 2,
        name: 'MacBook',
        price: 1999,
      },
    ],
  });
});

app.post("/api/order", async (req, res) => {
  try {
    if (!isValidProductId(req.body.productId)) {
      return res.status(400).json({
        message: "productId is required to create an order",
      });
    }

    const order = {
      id: Date.now(),
      productId: req.body.productId,
      quantity: req.body.quantity,
    };

    // Save order to database here

    await publishOrderCreated(order);

    res.status(201).json({
      message: "Order created",
      order,
    });

  } catch (error) {
    console.error(error);

    res.status(500).json({
      message: "Failed to create order",
    });
  }
});

async function start() {

  await connectProducer();

  app.listen(3003, () => {
    console.log("Order service running on port 3003");
  });

}

start();
