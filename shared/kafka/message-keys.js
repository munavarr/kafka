function isValidProductId(productId) {
  return productId !== undefined
    && productId !== null
    && String(productId).trim().length > 0;
}

function getOrderCreatedMessageKey(order) {
  if (!isValidProductId(order.productId)) {
    throw new Error("order.created requires a productId message key");
  }

  return `product:${String(order.productId).trim()}`;
}

module.exports = {
  getOrderCreatedMessageKey,
  isValidProductId,
};
