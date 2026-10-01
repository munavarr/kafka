const express = require('express');

const app = express();

app.use(express.json());

app.get('/api/products', (req, res) => {
  res.json({
    service: 'product-service',
    products: [
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

app.get('/api/products/:id', (req, res) => {
  res.json({
    service: 'product-service',
    productId: req.params.id,
  });
});

app.listen(3002, () => {
  console.log('Product Service running on port 3002');
});