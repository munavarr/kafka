function getConsumerOptions(groupId, groupInstanceId) {
  const memberId = groupInstanceId?.trim();

  return {
    groupId,
    rebalanceTimeout: 60_000,
    ...(memberId ? { groupInstanceId: memberId } : {}),
  };
}

function registerRebalanceLogging(consumer, serviceName) {
  consumer.on(consumer.events.REBALANCING, ({ payload }) => {
    console.log(`${serviceName} consumer group is rebalancing`, {
      groupId: payload.groupId,
      memberId: payload.memberId,
    });
  });

  consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
    console.log(`${serviceName} consumer group assignment completed`, {
      groupId: payload.groupId,
      memberId: payload.memberId,
      leaderId: payload.leaderId,
      isLeader: payload.isLeader,
      memberAssignment: payload.memberAssignment,
    });
  });
}

module.exports = {
  getConsumerOptions,
  registerRebalanceLogging,
};
