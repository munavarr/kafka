const express = require('express');

const app = express();

app.use(express.json());

app.get('/api/users', (req, res) => {
  res.json({
    service: 'user-service',
    users: [
      {
        id: 1,
        name: 'John',
      },
    ],
  });
});

app.get('/api/users/:id', (req, res) => {
  res.json({
    service: 'user-service',
    userId: req.params.id,
  });
});

app.listen(3001, () => {
  console.log('User Service running on port 3001');
});