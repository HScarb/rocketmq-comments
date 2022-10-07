/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.broker.processor;

import com.alibaba.fastjson.JSON;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import io.opentelemetry.api.common.Attributes;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.filter.ConsumerFilterData;
import org.apache.rocketmq.broker.filter.ConsumerFilterManager;
import org.apache.rocketmq.broker.filter.ExpressionMessageFilter;
import org.apache.rocketmq.broker.longpolling.PopRequest;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.broker.pagecache.ManyMessageTransfer;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.metrics.RemotingMetricsManager;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.filter.FilterAPI;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.PopCheckPoint;

import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_CONSUMER_GROUP;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_IS_RETRY;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_IS_SYSTEM;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_TOPIC;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_REQUEST_CODE;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_RESPONSE_CODE;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_RESULT;

/**
 * POP 模式拉取消息处理器
 */
public class PopMessageProcessor implements NettyRequestProcessor {
    private static final Logger POP_LOGGER =
        LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    private final BrokerController brokerController;
    private final Random random = new Random(System.currentTimeMillis());
    /**
     * 集群维度 ReviveTopic：rmg_sys_REVIVE_LOG_{CLUSTER_NAME}
     */
    String reviveTopic;
    private static final String BORN_TIME = "bornTime";

    private static final int POLLING_SUC = 0;
    private static final int POLLING_FULL = 1;
    private static final int POLLING_TIMEOUT = 2;
    private static final int NOT_POLLING = 3;

    // Topic 对应的消费者组
    private ConcurrentHashMap<String, ConcurrentHashMap<String, Byte>> topicCidMap;
    // 长轮询的 Pop 请求表，每个队列默认最大 1024 个等待中的 Pop 请求
    private ConcurrentLinkedHashMap<String, ConcurrentSkipListSet<PopRequest>> pollingMap;
    // 长轮询等待中的消费者数量，默认最大值为 10w
    private AtomicLong totalPollingNum = new AtomicLong(0);
    private PopLongPollingService popLongPollingService;
    private PopBufferMergeService popBufferMergeService;
    private QueueLockManager queueLockManager;
    private AtomicLong ckMessageNumber;

    public PopMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        // rmg_sys_REVIVE_LOG_{CLUSTER_NAME}
        this.reviveTopic = PopAckConstants.buildClusterReviveTopic(this.brokerController.getBrokerConfig().getBrokerClusterName());
        // 100000 topic default,  100000 lru topic + cid + qid
        this.topicCidMap = new ConcurrentHashMap<>(this.brokerController.getBrokerConfig().getPopPollingMapSize());
        this.pollingMap = new ConcurrentLinkedHashMap.Builder<String, ConcurrentSkipListSet<PopRequest>>()
            .maximumWeightedCapacity(this.brokerController.getBrokerConfig().getPopPollingMapSize()).build();
        this.popLongPollingService = new PopLongPollingService();
        this.queueLockManager = new QueueLockManager();
        this.popBufferMergeService = new PopBufferMergeService(this.brokerController, this);
        this.ckMessageNumber = new AtomicLong();
    }

    public PopLongPollingService getPopLongPollingService() {
        return popLongPollingService;
    }

    public PopBufferMergeService getPopBufferMergeService() {
        return this.popBufferMergeService;
    }

    public QueueLockManager getQueueLockManager() {
        return queueLockManager;
    }

    public static String genAckUniqueId(AckMsg ackMsg) {
        return ackMsg.getTopic()
            + PopAckConstants.SPLIT + ackMsg.getQueueId()
            + PopAckConstants.SPLIT + ackMsg.getAckOffset()
            + PopAckConstants.SPLIT + ackMsg.getConsumerGroup()
            + PopAckConstants.SPLIT + ackMsg.getPopTime()
            + PopAckConstants.SPLIT + ackMsg.getBrokerName()
            + PopAckConstants.SPLIT + PopAckConstants.ACK_TAG;
    }

    public static String genCkUniqueId(PopCheckPoint ck) {
        return ck.getTopic()
            + PopAckConstants.SPLIT + ck.getQueueId()
            + PopAckConstants.SPLIT + ck.getStartOffset()
            + PopAckConstants.SPLIT + ck.getCId()
            + PopAckConstants.SPLIT + ck.getPopTime()
            + PopAckConstants.SPLIT + ck.getBrokerName()
            + PopAckConstants.SPLIT + PopAckConstants.CK_TAG;
    }

    /**
     * 处理 POP 消息请求
     * @param ctx
     * @param request
     * @return
     * @throws RemotingCommandException
     */
    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        // 添加请求处理开始时间
        request.addExtField(BORN_TIME, String.valueOf(System.currentTimeMillis()));
        return this.processRequest(ctx.channel(), request);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    public ConcurrentLinkedHashMap<String, ConcurrentSkipListSet<PopRequest>> getPollingMap() {
        return pollingMap;
    }

    public void notifyLongPollingRequestIfNeed(String topic, String group, int queueId) {
        long popBufferOffset = this.brokerController.getPopMessageProcessor().getPopBufferMergeService().getLatestOffset(topic, group, queueId);
        long consumerOffset = this.brokerController.getConsumerOffsetManager().queryOffset(group, topic, queueId);
        long maxOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
        long offset = Math.max(popBufferOffset, consumerOffset);
        if (maxOffset > offset) {
            boolean notifySuccess = this.brokerController.getPopMessageProcessor().notifyMessageArriving(topic, group, -1);
            if (!notifySuccess) {
                // notify pop queue
                notifySuccess = this.brokerController.getPopMessageProcessor().notifyMessageArriving(topic, group, queueId);
            }
            this.brokerController.getNotificationProcessor().notifyMessageArriving(topic, queueId);
            if (this.brokerController.getBrokerConfig().isEnablePopLog()) {
                POP_LOGGER.info("notify long polling request. topic:{}, group:{}, queueId:{}, success:{}",
                    topic, group, queueId, notifySuccess);
            }
        }
    }

    public void notifyMessageArriving(final String topic, final int queueId) {
        ConcurrentHashMap<String, Byte> cids = topicCidMap.get(topic);
        if (cids == null) {
            return;
        }
        for (Entry<String, Byte> cid : cids.entrySet()) {
            if (queueId >= 0) {
                notifyMessageArriving(topic, cid.getKey(), -1);
            }
            notifyMessageArriving(topic, cid.getKey(), queueId);
        }
    }

    public boolean notifyMessageArriving(final String topic, final String cid, final int queueId) {
        ConcurrentSkipListSet<PopRequest> remotingCommands = pollingMap.get(KeyBuilder.buildPollingKey(topic, cid, queueId));
        if (remotingCommands == null || remotingCommands.isEmpty()) {
            return false;
        }
        PopRequest popRequest = remotingCommands.pollFirst();
        //clean inactive channel
        while (popRequest != null && !popRequest.getChannel().isActive()) {
            totalPollingNum.decrementAndGet();
            popRequest = remotingCommands.pollFirst();
        }

        if (popRequest == null) {
            return false;
        }
        totalPollingNum.decrementAndGet();
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("lock release , new msg arrive , wakeUp : {}", popRequest);
        }
        return wakeUp(popRequest);
    }

    private boolean wakeUp(final PopRequest request) {
        if (request == null || !request.complete()) {
            return false;
        }
        if (!request.getChannel().isActive()) {
            return false;
        }
        Runnable run = () -> {
            try {
                final RemotingCommand response = processRequest(request.getChannel(), request.getRemotingCommand());
                if (response != null) {
                    response.setOpaque(request.getRemotingCommand().getOpaque());
                    response.markResponseType();
                    NettyRemotingAbstract.writeResponse(request.getChannel(), request.getRemotingCommand(), response, future -> {
                        if (!future.isSuccess()) {
                            POP_LOGGER.error("ProcessRequestWrapper response to {} failed", request.getChannel().remoteAddress(), future.cause());
                            POP_LOGGER.error(request.toString());
                            POP_LOGGER.error(response.toString());
                        }
                    });
                }
            } catch (RemotingCommandException e1) {
                POP_LOGGER.error("ExecuteRequestWhenWakeup run", e1);
            }
        };
        this.brokerController.getPullMessageExecutor().submit(new RequestTask(run, request.getChannel(), request.getRemotingCommand()));
        return true;
    }

    /**
     * 处理 POP 消息请求
     *
     * @param channel
     * @param request
     * @return
     * @throws RemotingCommandException
     */
    private RemotingCommand processRequest(final Channel channel, RemotingCommand request)
        throws RemotingCommandException {
        RemotingCommand response = RemotingCommand.createResponseCommand(PopMessageResponseHeader.class);
        final PopMessageResponseHeader responseHeader = (PopMessageResponseHeader) response.readCustomHeader();
        final PopMessageRequestHeader requestHeader =
            (PopMessageRequestHeader) request.decodeCommandCustomHeader(PopMessageRequestHeader.class);
        StringBuilder startOffsetInfo = new StringBuilder(64);
        StringBuilder msgOffsetInfo = new StringBuilder(64);
        StringBuilder orderCountInfo = null;
        if (requestHeader.isOrder()) {
            orderCountInfo = new StringBuilder(64);
        }

        brokerController.getConsumerManager().compensateBasicConsumerInfo(requestHeader.getConsumerGroup(),
            ConsumeType.CONSUME_POP, MessageModel.CLUSTERING);

        response.setOpaque(request.getOpaque());

        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("receive PopMessage request command, {}", request);
        }

        // Broker 处理 pop 请求超时
        if (requestHeader.isTimeoutTooMuch()) {
            response.setCode(ResponseCode.POLLING_TIMEOUT);
            response.setRemark(String.format("the broker[%s] poping message is timeout too much",
                this.brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }
        // Broker 不可读
        if (!PermName.isReadable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(String.format("the broker[%s] poping message is forbidden",
                this.brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }
        // Pop 消息数量大于 32
        if (requestHeader.getMaxMsgNums() > 32) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("the broker[%s] poping message's num is greater than 32",
                this.brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }

        // Pop 的 Topic
        TopicConfig topicConfig =
            this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            POP_LOGGER.error("The topic {} not exist, consumer: {} ", requestHeader.getTopic(),
                RemotingHelper.parseChannelRemoteAddr(channel));
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(String.format("topic[%s] not exist, apply first please! %s", requestHeader.getTopic(),
                FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL)));
            return response;
        }

        // Topic 不可读
        if (!PermName.isReadable(topicConfig.getPerm())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the topic[" + requestHeader.getTopic() + "] peeking message is forbidden");
            return response;
        }

        // Pop 队列Id 大于 Topic 队列数
        if (requestHeader.getQueueId() >= topicConfig.getReadQueueNums()) {
            String errorInfo = String.format("queueId[%d] is illegal, topic:[%s] topicConfig.readQueueNums:[%d] " +
                    "consumer:[%s]",
                requestHeader.getQueueId(), requestHeader.getTopic(), topicConfig.getReadQueueNums(),
                channel.remoteAddress());
            POP_LOGGER.warn(errorInfo);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errorInfo);
            return response;
        }
        // 订阅信息
        SubscriptionGroupConfig subscriptionGroupConfig =
            this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(requestHeader.getConsumerGroup());
        if (null == subscriptionGroupConfig) {
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark(String.format("subscription group [%s] does not exist, %s",
                requestHeader.getConsumerGroup(), FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST)));
            return response;
        }

        // 消费组不可消费
        if (!subscriptionGroupConfig.isConsumeEnable()) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("subscription group no permission, " + requestHeader.getConsumerGroup());
            return response;
        }

        // 消息过滤
        ExpressionMessageFilter messageFilter = null;
        if (requestHeader.getExp() != null && requestHeader.getExp().length() > 0) {
            try {
                SubscriptionData subscriptionData = FilterAPI.build(requestHeader.getTopic(), requestHeader.getExp(), requestHeader.getExpType());
                brokerController.getConsumerManager().compensateSubscribeData(requestHeader.getConsumerGroup(),
                    requestHeader.getTopic(), subscriptionData);

                String retryTopic = KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup());
                SubscriptionData retrySubscriptionData = FilterAPI.build(retryTopic, SubscriptionData.SUB_ALL, requestHeader.getExpType());
                brokerController.getConsumerManager().compensateSubscribeData(requestHeader.getConsumerGroup(),
                    retryTopic, retrySubscriptionData);

                ConsumerFilterData consumerFilterData = null;
                if (!ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                    consumerFilterData = ConsumerFilterManager.build(
                        requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getExp(),
                        requestHeader.getExpType(), System.currentTimeMillis()
                    );
                    if (consumerFilterData == null) {
                        POP_LOGGER.warn("Parse the consumer's subscription[{}] failed, group: {}",
                            requestHeader.getExp(), requestHeader.getConsumerGroup());
                        response.setCode(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
                        response.setRemark("parse the consumer's subscription failed");
                        return response;
                    }
                }
                messageFilter = new ExpressionMessageFilter(subscriptionData, consumerFilterData,
                    brokerController.getConsumerFilterManager());
            } catch (Exception e) {
                POP_LOGGER.warn("Parse the consumer's subscription[{}] error, group: {}", requestHeader.getExp(),
                    requestHeader.getConsumerGroup());
                response.setCode(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
                response.setRemark("parse the consumer's subscription failed");
                return response;
            }
        } else {
            try {
                SubscriptionData subscriptionData = FilterAPI.build(requestHeader.getTopic(), "*", ExpressionType.TAG);
                brokerController.getConsumerManager().compensateSubscribeData(requestHeader.getConsumerGroup(),
                    requestHeader.getTopic(), subscriptionData);

                String retryTopic = KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup());
                SubscriptionData retrySubscriptionData = FilterAPI.build(retryTopic, "*", ExpressionType.TAG);
                brokerController.getConsumerManager().compensateSubscribeData(requestHeader.getConsumerGroup(),
                    retryTopic, retrySubscriptionData);
            } catch (Exception e) {
                POP_LOGGER.warn("Build default subscription error, group: {}", requestHeader.getConsumerGroup());
            }
        }

        // 生成随机数
        int randomQ = random.nextInt(100);
        int reviveQid;
        if (requestHeader.isOrder()) {
            reviveQid = KeyBuilder.POP_ORDER_REVIVE_QUEUE;
        } else {
            // 轮询选一个 Revive 队列
            reviveQid = (int) Math.abs(ckMessageNumber.getAndIncrement() % this.brokerController.getBrokerConfig().getReviveQueueNum());
        }

        int commercialSizePerMsg = this.brokerController.getBrokerConfig().getCommercialSizePerMsg();
        GetMessageResult getMessageResult = new GetMessageResult(commercialSizePerMsg);
        ExpressionMessageFilter finalMessageFilter = messageFilter;
        StringBuilder finalOrderCountInfo = orderCountInfo;

        // 1/5 的概率拉取重试消息
        boolean needRetry = randomQ % 5 == 0;
        long popTime = System.currentTimeMillis();
        CompletableFuture<Long> getMessageFuture = CompletableFuture.completedFuture(0L);
        // 拉取重试消息
        if (needRetry && !requestHeader.isOrder()) {
            TopicConfig retryTopicConfig =
                this.brokerController.getTopicConfigManager().selectTopicConfig(KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup()));
            if (retryTopicConfig != null) {
                for (int i = 0; i < retryTopicConfig.getReadQueueNums(); i++) {
                    int queueId = (randomQ + i) % retryTopicConfig.getReadQueueNums();
                    getMessageFuture = getMessageFuture.thenCompose(restNum -> popMsgFromQueue(true, getMessageResult, requestHeader, queueId, restNum, reviveQid, channel, popTime, finalMessageFilter,
                        startOffsetInfo, msgOffsetInfo, finalOrderCountInfo));
                }
            }
        }
        // 如果拉取请求没有指定队列（-1），则拉取所有队列
        if (requestHeader.getQueueId() < 0) {
            // 从 randomQ 队列开始拉取所有队列的消息
            // read all queue
            for (int i = 0; i < topicConfig.getReadQueueNums(); i++) {
                int queueId = (randomQ + i) % topicConfig.getReadQueueNums();
                getMessageFuture = getMessageFuture.thenCompose(restNum -> popMsgFromQueue(false, getMessageResult, requestHeader, queueId, restNum, reviveQid, channel, popTime, finalMessageFilter,
                    startOffsetInfo, msgOffsetInfo, finalOrderCountInfo));
            }
        } else {
            // 拉取请求指定了队列，拉取对应的队列
            int queueId = requestHeader.getQueueId();
            getMessageFuture = getMessageFuture.thenCompose(restNum -> popMsgFromQueue(false, getMessageResult, requestHeader, queueId, restNum, reviveQid, channel, popTime, finalMessageFilter,
                startOffsetInfo, msgOffsetInfo, finalOrderCountInfo));
        }
        // 如果前面拉取普通消息之后，没有满，则再拉取一次重试消息
        // if not full , fetch retry again
        if (!needRetry && getMessageResult.getMessageMapedList().size() < requestHeader.getMaxMsgNums() && !requestHeader.isOrder()) {
            TopicConfig retryTopicConfig =
                this.brokerController.getTopicConfigManager().selectTopicConfig(KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup()));
            if (retryTopicConfig != null) {
                for (int i = 0; i < retryTopicConfig.getReadQueueNums(); i++) {
                    int queueId = (randomQ + i) % retryTopicConfig.getReadQueueNums();
                    getMessageFuture = getMessageFuture.thenCompose(restNum -> popMsgFromQueue(true, getMessageResult, requestHeader, queueId, restNum, reviveQid, channel, popTime, finalMessageFilter,
                        startOffsetInfo, msgOffsetInfo, finalOrderCountInfo));
                }
            }
        }

        final RemotingCommand finalResponse = response;
        getMessageFuture.thenApply(restNum -> {
            if (!getMessageResult.getMessageBufferList().isEmpty()) {
                // 拉取消息成功
                finalResponse.setCode(ResponseCode.SUCCESS);
                getMessageResult.setStatus(GetMessageStatus.FOUND);
                if (restNum > 0) {
                    // all queue pop can not notify specified queue pop, and vice versa
                    notifyMessageArriving(requestHeader.getTopic(), requestHeader.getConsumerGroup(),
                        requestHeader.getQueueId());
                }
            } else {
                // 没有拉取到消息，长轮询
                int pollingResult = polling(channel, request, requestHeader);
                if (POLLING_SUC == pollingResult) {
                    return null;
                } else if (POLLING_FULL == pollingResult) {
                    finalResponse.setCode(ResponseCode.POLLING_FULL);
                } else {
                    finalResponse.setCode(ResponseCode.POLLING_TIMEOUT);
                }
                getMessageResult.setStatus(GetMessageStatus.NO_MESSAGE_IN_QUEUE);
            }
            responseHeader.setInvisibleTime(requestHeader.getInvisibleTime());
            responseHeader.setPopTime(popTime);
            responseHeader.setReviveQid(reviveQid);
            responseHeader.setRestNum(restNum);
            responseHeader.setStartOffsetInfo(startOffsetInfo.toString());
            responseHeader.setMsgOffsetInfo(msgOffsetInfo.toString());
            if (requestHeader.isOrder() && finalOrderCountInfo != null) {
                responseHeader.setOrderCountInfo(finalOrderCountInfo.toString());
            }
            finalResponse.setRemark(getMessageResult.getStatus().name());
            switch (finalResponse.getCode()) {
                case ResponseCode.SUCCESS:
                    if (this.brokerController.getBrokerConfig().isTransferMsgByHeap()) {
                        // 用堆内存传输消息，将消息 byte buffer 设置为 response body 返回
                        final long beginTimeMills = this.brokerController.getMessageStore().now();
                        final byte[] r = this.readGetMessageResult(getMessageResult, requestHeader.getConsumerGroup(),
                            requestHeader.getTopic(), requestHeader.getQueueId());
                        this.brokerController.getBrokerStatsManager().incGroupGetLatency(requestHeader.getConsumerGroup(),
                            requestHeader.getTopic(), requestHeader.getQueueId(),
                            (int) (this.brokerController.getMessageStore().now() - beginTimeMills));
                        finalResponse.setBody(r);
                    } else {
                        // 用堆外内存传输消息，构造 FileRegion，实现 transferTo 方法达到零拷贝，直接返回
                        final GetMessageResult tmpGetMessageResult = getMessageResult;
                        try {
                            FileRegion fileRegion =
                                new ManyMessageTransfer(finalResponse.encodeHeader(getMessageResult.getBufferTotalSize()),
                                    getMessageResult);
                            channel.writeAndFlush(fileRegion)
                                .addListener((ChannelFutureListener) future -> {
                                    tmpGetMessageResult.release();
                                    Attributes attributes = RemotingMetricsManager.newAttributesBuilder()
                                        .put(LABEL_REQUEST_CODE, RemotingMetricsManager.getRequestCodeDesc(request.getCode()))
                                        .put(LABEL_RESPONSE_CODE, RemotingMetricsManager.getResponseCodeDesc(finalResponse.getCode()))
                                        .put(LABEL_RESULT, RemotingMetricsManager.getWriteAndFlushResult(future))
                                        .build();
                                    RemotingMetricsManager.rpcLatency.record(request.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributes);
                                    if (!future.isSuccess()) {
                                        POP_LOGGER.error("Fail to transfer messages from page cache to {}",
                                            channel.remoteAddress(), future.cause());
                                    }
                                });
                        } catch (Throwable e) {
                            POP_LOGGER.error("Error occurred when transferring messages from page cache", e);
                            getMessageResult.release();
                        }

                        return null;
                    }
                    break;
                default:
                    return finalResponse;
            }
            return finalResponse;
        }).thenAccept(result -> NettyRemotingAbstract.writeResponse(channel, request, result));
        return null;
    }

    /**
     * 从消息队列中 POP 消息
     *
     * @param isRetry 是否是重试 Topic
     * @param getMessageResult
     * @param requestHeader
     * @param queueId 消息队列 ID
     * @param restNum 队列剩余消息数量
     * @param reviveQid 唤醒队列 ID
     * @param channel Netty Channel，用于获取客户端 host，来提交消费进度
     * @param popTime Pop 时间
     * @param messageFilter
     * @param startOffsetInfo 获取 Pop 的起始偏移量
     * @param msgOffsetInfo 获取所有 Pop 的消息的逻辑偏移量
     * @param orderCountInfo
     * @return
     */
    private CompletableFuture<Long> popMsgFromQueue(boolean isRetry, GetMessageResult getMessageResult,
        PopMessageRequestHeader requestHeader, int queueId, long restNum, int reviveQid,
        Channel channel, long popTime, ExpressionMessageFilter messageFilter, StringBuilder startOffsetInfo,
        StringBuilder msgOffsetInfo, StringBuilder orderCountInfo) {
        String topic = isRetry ? KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(),
            requestHeader.getConsumerGroup()) : requestHeader.getTopic();
        // {TOPIC}@{GROUP}@{QUEUE_ID}
        String lockKey =
            topic + PopAckConstants.SPLIT + requestHeader.getConsumerGroup() + PopAckConstants.SPLIT + queueId;
        boolean isOrder = requestHeader.isOrder();
        long offset = getPopOffset(topic, requestHeader.getConsumerGroup(), queueId, requestHeader.getInitMode(),
            false, lockKey, false);
        CompletableFuture<Long> future = new CompletableFuture<>();
        // Queue 上加锁，保证同一时刻只有一个消费者可以拉取同一个 Queue 的消息
        if (!queueLockManager.tryLock(lockKey)) {
            // 返回该队列中待 Pop 的消息数量
            restNum = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - offset + restNum;
            future.complete(restNum);
            return future;
        }

        try {
            future.whenComplete((result, throwable) -> queueLockManager.unLock(lockKey));
            // 计算要 POP 的消息偏移量
            offset = getPopOffset(topic, requestHeader.getConsumerGroup(), queueId, requestHeader.getInitMode(),
                true, lockKey, true);
            // 顺序消费，阻塞
            if (isOrder && brokerController.getConsumerOrderInfoManager().checkBlock(topic,
                requestHeader.getConsumerGroup(), queueId, requestHeader.getInvisibleTime())) {
                future.complete(this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - offset + restNum);
                return future;
            }

            if (isOrder) {
                this.brokerController.getPopInflightMessageCounter().clearInFlightMessageNum(
                    topic,
                    requestHeader.getConsumerGroup(),
                    queueId
                );
            }

            // 已经拉取到足够的消息
            if (getMessageResult.getMessageMapedList().size() >= requestHeader.getMaxMsgNums()) {
                restNum = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - offset + restNum;
                future.complete(restNum);
                return future;
            }
        } catch (Exception e) {
            POP_LOGGER.error("Exception in popMsgFromQueue", e);
            future.complete(restNum);
            return future;
        }

        AtomicLong atomicRestNum = new AtomicLong(restNum);
        AtomicLong atomicOffset = new AtomicLong(offset);
        long finalOffset = offset;
        // 从磁盘消息存储中根据逻辑偏移量查询消息
        return this.brokerController.getMessageStore()
            .getMessageAsync(requestHeader.getConsumerGroup(), topic, queueId, offset,
                requestHeader.getMaxMsgNums() - getMessageResult.getMessageMapedList().size(), messageFilter)
            .thenCompose(result -> {
                if (result == null) {
                    return CompletableFuture.completedFuture(null);
                }
                // maybe store offset is not correct.
                if (GetMessageStatus.OFFSET_TOO_SMALL.equals(result.getStatus())
                    || GetMessageStatus.OFFSET_OVERFLOW_BADLY.equals(result.getStatus())
                    || GetMessageStatus.OFFSET_FOUND_NULL.equals(result.getStatus())) {
                    // commit offset, because the offset is not correct
                    // If offset in store is greater than cq offset, it will cause duplicate messages,
                    // because offset in PopBuffer is not committed.
                    POP_LOGGER.warn("Pop initial offset, because store is no correct, {}, {}->{}",
                        lockKey, atomicOffset.get(), result.getNextBeginOffset());
                    this.brokerController.getConsumerOffsetManager().commitOffset(channel.remoteAddress().toString(), requestHeader.getConsumerGroup(), topic,
                        queueId, result.getNextBeginOffset());
                    atomicOffset.set(result.getNextBeginOffset());
                    return this.brokerController.getMessageStore().getMessageAsync(requestHeader.getConsumerGroup(), topic, queueId, atomicOffset.get(),
                        requestHeader.getMaxMsgNums() - getMessageResult.getMessageMapedList().size(), messageFilter);
                }
                return CompletableFuture.completedFuture(result);
            }).thenApply(result -> {
                if (result == null) {
                    atomicRestNum.set(brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - atomicOffset.get() + atomicRestNum.get());
                    return atomicRestNum.get();
                }
                if (!result.getMessageMapedList().isEmpty()) {
                    // 更新统计数据
                    this.brokerController.getBrokerStatsManager().incBrokerGetNums(requestHeader.getTopic(), result.getMessageCount());
                    this.brokerController.getBrokerStatsManager().incGroupGetNums(requestHeader.getConsumerGroup(), topic,
                        result.getMessageCount());
                    this.brokerController.getBrokerStatsManager().incGroupGetSize(requestHeader.getConsumerGroup(), topic,
                        result.getBufferTotalSize());

                    Attributes attributes = BrokerMetricsManager.newAttributesBuilder()
                        .put(LABEL_TOPIC, requestHeader.getTopic())
                        .put(LABEL_CONSUMER_GROUP, requestHeader.getConsumerGroup())
                        .put(LABEL_IS_SYSTEM, TopicValidator.isSystemTopic(requestHeader.getTopic()) || MixAll.isSysConsumerGroup(requestHeader.getConsumerGroup()))
                        .put(LABEL_IS_RETRY, isRetry)
                        .build();
                    BrokerMetricsManager.messagesOutTotal.add(result.getMessageCount(), attributes);
                    BrokerMetricsManager.throughputOutTotal.add(result.getBufferTotalSize(), attributes);

                    if (isOrder) {
                    // 顺序消费，更新偏移量
                        this.brokerController.getConsumerOrderInfoManager().update(isRetry, topic,
                            requestHeader.getConsumerGroup(),
                            queueId, popTime, requestHeader.getInvisibleTime(), result.getMessageQueueOffset(),
                            orderCountInfo);
                        this.brokerController.getConsumerOffsetManager().commitOffset(channel.remoteAddress().toString(),
                            requestHeader.getConsumerGroup(), topic, queueId, finalOffset);
                    } else {
                        // 添加 CheckPoint 到内存，用于等待 ACK
                        appendCheckPoint(requestHeader, topic, reviveQid, queueId, finalOffset, result, popTime, this.brokerController.getBrokerConfig().getBrokerName());
                    }
                    ExtraInfoUtil.buildStartOffsetInfo(startOffsetInfo, isRetry, queueId, finalOffset);
                    ExtraInfoUtil.buildMsgOffsetInfo(msgOffsetInfo, isRetry, queueId,
                        result.getMessageQueueOffset());
                } else if ((GetMessageStatus.NO_MATCHED_MESSAGE.equals(result.getStatus())
                    || GetMessageStatus.OFFSET_FOUND_NULL.equals(result.getStatus())
                    || GetMessageStatus.MESSAGE_WAS_REMOVING.equals(result.getStatus())
                    || GetMessageStatus.NO_MATCHED_LOGIC_QUEUE.equals(result.getStatus()))
                    && result.getNextBeginOffset() > -1) {
                    // 没有拉取到消息，添加假的消息 CheckPoint 到队列
                    popBufferMergeService.addCkMock(requestHeader.getConsumerGroup(), topic, queueId, finalOffset,
                        requestHeader.getInvisibleTime(), popTime, reviveQid, result.getNextBeginOffset(), brokerController.getBrokerConfig().getBrokerName());
//                this.brokerController.getConsumerOffsetManager().commitOffset(channel.remoteAddress().toString(), requestHeader.getConsumerGroup(), topic,
//                        queueId, getMessageTmpResult.getNextBeginOffset());
                }

                atomicRestNum.set(result.getMaxOffset() - result.getNextBeginOffset() + atomicRestNum.get());
                String brokerName = brokerController.getBrokerConfig().getBrokerName();
                for (SelectMappedBufferResult mapedBuffer : result.getMessageMapedList()) {
                    // We should not recode buffer for normal topic message
                    if (!isRetry) {
                        getMessageResult.addMessage(mapedBuffer);
                    } else {
                        List<MessageExt> messageExtList = MessageDecoder.decodesBatch(mapedBuffer.getByteBuffer(),
                            true, false, true);
                        mapedBuffer.release();
                        for (MessageExt messageExt : messageExtList) {
                            try {
                                String ckInfo = ExtraInfoUtil.buildExtraInfo(finalOffset, popTime, requestHeader.getInvisibleTime(),
                                    reviveQid, messageExt.getTopic(), brokerName, messageExt.getQueueId(), messageExt.getQueueOffset());
                                messageExt.getProperties().putIfAbsent(MessageConst.PROPERTY_POP_CK, ckInfo);

                                // Set retry message topic to origin topic and clear message store size to recode
                                messageExt.setTopic(requestHeader.getTopic());
                                messageExt.setStoreSize(0);

                                byte[] encode = MessageDecoder.encode(messageExt, false);
                                ByteBuffer buffer = ByteBuffer.wrap(encode);
                                // 将拉取到的消息放入结果容器中
                                SelectMappedBufferResult tmpResult =
                                    new SelectMappedBufferResult(mapedBuffer.getStartOffset(), buffer, encode.length, null);
                                getMessageResult.addMessage(tmpResult);
                            } catch (Exception e) {
                                POP_LOGGER.error("Exception in recode retry message buffer, topic={}", topic, e);
                            }
                        }
                    }
                }
                this.brokerController.getPopInflightMessageCounter().incrementInFlightMessageNum(
                    topic,
                    requestHeader.getConsumerGroup(),
                    queueId,
                    result.getMessageCount()
                );
                return atomicRestNum.get();
            }).whenComplete((result, throwable) -> {
                if (throwable != null) {
                    POP_LOGGER.error("Pop message error, {}", lockKey, throwable);
                }
                // Pop 完后解锁
                queueLockManager.unLock(lockKey);
            });
    }

    /**
     * 获取 POP 消费拉取偏移量
     *
     * @param topic
     * @param requestHeader
     * @param queueId
     * @param init  false：仅查询，不更新偏移量；true：查询并更新偏移量
     * @param lockKey 队列锁定 Key：{TOPIC}@{GROUP}@{QUEUE_ID}
     * @return
     */
    private long getPopOffset(String topic, String group, int queueId, int initMode, boolean init, String lockKey,
        boolean checkResetOffset) {

        long offset = this.brokerController.getConsumerOffsetManager().queryOffset(group, topic, queueId);
        if (offset < 0) {
            // 如果是首次消费，根据消费初始化模式返回偏移量
            if (ConsumeInitMode.MIN == initMode) {
                // 从头开始，返回队列的最小偏移量
                offset = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, queueId);
            } else {
                // 如果从最新的 offset 拉取，Pop 最后一条消息，然后提交偏移量
                // pop last one,then commit offset.
                offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - 1;
                // max & no consumer offset
                if (offset < 0) {
                    offset = 0;
                }
                // 提交消费偏移量
                if (init) {
                    this.brokerController.getConsumerOffsetManager().commitOffset(
                        "getPopOffset", group, topic, queueId, offset);
                }
            }
        }

        if (checkResetOffset) {
            Long resetOffset = resetPopOffset(topic, group, queueId);
            if (resetOffset != null) {
                return resetOffset;
            }
        }

        // 查询 POP CheckPoint 与 ACK 消息匹配后的最新位点
        long bufferOffset = this.popBufferMergeService.getLatestOffset(lockKey);
        if (bufferOffset < 0) {
            return offset;
        } else {
            // 返回更大的位点
            return Math.max(bufferOffset, offset);
        }
    }

    /**
     * 长轮询，等待有消息可以 Pop
     *
     * @param channel
     * @param remotingCommand
     * @param requestHeader
     * @return
     */
    private int polling(final Channel channel, RemotingCommand remotingCommand,
        final PopMessageRequestHeader requestHeader) {
        // 根据长轮询等待时间判断是否要长轮询
        if (requestHeader.getPollTime() <= 0 || this.popLongPollingService.isStopped()) {
            return NOT_POLLING;
        }
        ConcurrentHashMap<String, Byte> cids = topicCidMap.get(requestHeader.getTopic());
        if (cids == null) {
            cids = new ConcurrentHashMap<>();
            ConcurrentHashMap<String, Byte> old = topicCidMap.putIfAbsent(requestHeader.getTopic(), cids);
            if (old != null) {
                cids = old;
            }
        }
        cids.putIfAbsent(requestHeader.getConsumerGroup(), Byte.MIN_VALUE);
        // 长轮询等待的最大时间戳
        long expired = requestHeader.getBornTime() + requestHeader.getPollTime();
        // 构造 Pop 请求
        final PopRequest request = new PopRequest(remotingCommand, channel, expired);
        // 长轮询等待的客户端超过数量上限
        boolean isFull = totalPollingNum.get() >= this.brokerController.getBrokerConfig().getMaxPopPollingSize();
        if (isFull) {
            POP_LOGGER.info("polling {}, result POLLING_FULL, total:{}", remotingCommand, totalPollingNum.get());
            return POLLING_FULL;
        }
        // 是否已经超时
        boolean isTimeout = request.isTimeout();
        if (isTimeout) {
            if (brokerController.getBrokerConfig().isEnablePopLog()) {
                POP_LOGGER.info("polling {}, result POLLING_TIMEOUT", remotingCommand);
            }
            return POLLING_TIMEOUT;
        }
        String key = KeyBuilder.buildPollingKey(requestHeader.getTopic(), requestHeader.getConsumerGroup(),
            requestHeader.getQueueId());
        // 从长轮询表中查询长轮询队列
        ConcurrentSkipListSet<PopRequest> queue = pollingMap.get(key);
        if (queue == null) {
            queue = new ConcurrentSkipListSet<>(PopRequest.COMPARATOR);
            ConcurrentSkipListSet<PopRequest> old = pollingMap.putIfAbsent(key, queue);
            if (old != null) {
                queue = old;
            }
        } else {
            // check size
            int size = queue.size();
            if (size > brokerController.getBrokerConfig().getPopPollingSize()) {
                POP_LOGGER.info("polling {}, result POLLING_FULL, singleSize:{}", remotingCommand, size);
                return POLLING_FULL;
            }
        }
        if (queue.add(request)) {
            remotingCommand.setSuspended(true);
            // 将 Pop 请求加入长轮询等待队列，增加等待中的长轮询请求数量
            totalPollingNum.incrementAndGet();
            if (brokerController.getBrokerConfig().isEnablePopLog()) {
                POP_LOGGER.info("polling {}, result POLLING_SUC", remotingCommand);
            }
            return POLLING_SUC;
        } else {
            POP_LOGGER.info("polling {}, result POLLING_FULL, add fail, {}", request, queue);
            return POLLING_FULL;
        }
    }

    /**
     * 构造 CK 消息，用于保存到磁盘
     * 该消息为定时消息，投递时间为 ReviveTime
     *
     * @param ck
     * @param reviveQid
     * @return
     */
    public final MessageExtBrokerInner buildCkMsg(final PopCheckPoint ck, final int reviveQid) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();

        msgInner.setTopic(reviveTopic);
        msgInner.setBody(JSON.toJSONString(ck).getBytes(DataConverter.charset));
        msgInner.setQueueId(reviveQid);
        msgInner.setTags(PopAckConstants.CK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(this.brokerController.getStoreHost());
        msgInner.setStoreHost(this.brokerController.getStoreHost());
        // 定时投递，设置投递时间为 ReviveTime - 1s
        msgInner.setDeliverTimeMs(ck.getReviveTime() - PopAckConstants.ackTimeInterval);
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, genCkUniqueId(ck));
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        return msgInner;
    }

    /**
     * 在 POP 拉取消息后调用，添加 CheckPoint，等待 ACK
     *
     * @param requestHeader
     * @param topic POP 的 Topic
     * @param reviveQid Revive 队列 ID
     * @param queueId POP 的队列 ID
     * @param offset POP 消息的起始偏移量
     * @param getMessageTmpResult POP 一批消息的结果
     * @param popTime POP 时间
     * @param brokerName
     */
    private void appendCheckPoint(final PopMessageRequestHeader requestHeader,
        final String topic, final int reviveQid, final int queueId, final long offset,
        final GetMessageResult getMessageTmpResult, final long popTime, final String brokerName) {
        // add check point msg to revive log
        final PopCheckPoint ck = new PopCheckPoint();
        ck.setBitMap(0);
        ck.setNum((byte) getMessageTmpResult.getMessageMapedList().size());
        ck.setPopTime(popTime);
        ck.setInvisibleTime(requestHeader.getInvisibleTime());
        ck.setStartOffset(offset);
        ck.setCId(requestHeader.getConsumerGroup());
        ck.setTopic(topic);
        ck.setQueueId(queueId);
        ck.setBrokerName(brokerName);
        for (Long msgQueueOffset : getMessageTmpResult.getMessageQueueOffset()) {
            // 添加所有拉取的消息的偏移量与起始偏移量的差值
            ck.addDiff((int) (msgQueueOffset - offset));
        }

        // 将 Offset 放入内存
        final boolean addBufferSuc = this.popBufferMergeService.addCk(
            ck, reviveQid, -1, getMessageTmpResult.getNextBeginOffset()
        );

        if (addBufferSuc) {
            return;
        }

        // 放入内存匹配失败（内存匹配未开启），将 Offset 放入内存和磁盘
        this.popBufferMergeService.addCkJustOffset(
            ck, reviveQid, -1, getMessageTmpResult.getNextBeginOffset()
        );
    }

    private Long resetPopOffset(String topic, String group, int queueId) {
        String lockKey = topic + PopAckConstants.SPLIT + group + PopAckConstants.SPLIT + queueId;
        Long resetOffset =
            this.brokerController.getConsumerOffsetManager().queryThenEraseResetOffset(topic, group, queueId);
        if (resetOffset != null) {
            this.brokerController.getConsumerOrderInfoManager().clearBlock(topic, group, queueId);
            this.getPopBufferMergeService().clearOffsetQueue(lockKey);
            this.brokerController.getConsumerOffsetManager()
                .commitOffset("ResetPopOffset", group, topic, queueId, resetOffset);
        }
        return resetOffset;
    }

    /**
     * 将消息查询结果放入 byte buffer
     *
     * @param getMessageResult 消息查询结果
     * @param group
     * @param topic
     * @param queueId
     * @return
     */
    private byte[] readGetMessageResult(final GetMessageResult getMessageResult, final String group, final String topic,
        final int queueId) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(getMessageResult.getBufferTotalSize());

        long storeTimestamp = 0;
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {

                byteBuffer.put(bb);
                storeTimestamp = bb.getLong(MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSITION);
            }
        } finally {
            getMessageResult.release();
        }

        // 记录消息保存和拉取的时间差
        this.brokerController.getBrokerStatsManager().recordDiskFallBehindTime(group, topic, queueId,
            this.brokerController.getMessageStore().now() - storeTimestamp);
        return byteBuffer.array();
    }

    public class PopLongPollingService extends ServiceThread {

        private long lastCleanTime = 0;

        @Override
        public String getServiceName() {
            if (PopMessageProcessor.this.brokerController.getBrokerConfig().isInBrokerContainer()) {
                return PopMessageProcessor.this.brokerController.getBrokerIdentity().getIdentifier() + PopLongPollingService.class.getSimpleName();
            }
            return PopLongPollingService.class.getSimpleName();
        }

        private void cleanUnusedResource() {
            try {
                {
                    Iterator<Entry<String, ConcurrentHashMap<String, Byte>>> topicCidMapIter = topicCidMap.entrySet().iterator();
                    while (topicCidMapIter.hasNext()) {
                        Entry<String, ConcurrentHashMap<String, Byte>> entry = topicCidMapIter.next();
                        String topic = entry.getKey();
                        if (brokerController.getTopicConfigManager().selectTopicConfig(topic) == null) {
                            POP_LOGGER.info("remove not exit topic {} in topicCidMap!", topic);
                            topicCidMapIter.remove();
                            continue;
                        }
                        Iterator<Entry<String, Byte>> cidMapIter = entry.getValue().entrySet().iterator();
                        while (cidMapIter.hasNext()) {
                            Entry<String, Byte> cidEntry = cidMapIter.next();
                            String cid = cidEntry.getKey();
                            if (!brokerController.getSubscriptionGroupManager().getSubscriptionGroupTable().containsKey(cid)) {
                                POP_LOGGER.info("remove not exit sub {} of topic {} in topicCidMap!", cid, topic);
                                cidMapIter.remove();
                            }
                        }
                    }
                }

                {
                    Iterator<Entry<String, ConcurrentSkipListSet<PopRequest>>> pollingMapIter = pollingMap.entrySet().iterator();
                    while (pollingMapIter.hasNext()) {
                        Entry<String, ConcurrentSkipListSet<PopRequest>> entry = pollingMapIter.next();
                        if (entry.getKey() == null) {
                            continue;
                        }
                        String[] keyArray = entry.getKey().split(PopAckConstants.SPLIT);
                        if (keyArray == null || keyArray.length != 3) {
                            continue;
                        }
                        String topic = keyArray[0];
                        String cid = keyArray[1];
                        if (brokerController.getTopicConfigManager().selectTopicConfig(topic) == null) {
                            POP_LOGGER.info("remove not exit topic {} in pollingMap!", topic);
                            pollingMapIter.remove();
                            continue;
                        }
                        if (!brokerController.getSubscriptionGroupManager().getSubscriptionGroupTable().containsKey(cid)) {
                            POP_LOGGER.info("remove not exit sub {} of topic {} in pollingMap!", cid, topic);
                            pollingMapIter.remove();
                            continue;
                        }
                    }
                }
            } catch (Throwable e) {
                POP_LOGGER.error("cleanUnusedResource", e);
            }

            lastCleanTime = System.currentTimeMillis();
        }

        @Override
        public void run() {
            int i = 0;
            while (!this.stopped) {
                try {
                    this.waitForRunning(20);
                    i++;
                    if (pollingMap.isEmpty()) {
                        continue;
                    }
                    long tmpTotalPollingNum = 0;
                    Iterator<Entry<String, ConcurrentSkipListSet<PopRequest>>> pollingMapIterator = pollingMap.entrySet().iterator();
                    while (pollingMapIterator.hasNext()) {
                        Entry<String, ConcurrentSkipListSet<PopRequest>> entry = pollingMapIterator.next();
                        String key = entry.getKey();
                        ConcurrentSkipListSet<PopRequest> popQ = entry.getValue();
                        if (popQ == null) {
                            continue;
                        }
                        PopRequest first;
                        do {
                            first = popQ.pollFirst();
                            if (first == null) {
                                break;
                            }
                            if (!first.isTimeout()) {
                                if (popQ.add(first)) {
                                    break;
                                } else {
                                    POP_LOGGER.info("polling, add fail again: {}", first);
                                }
                            }
                            if (brokerController.getBrokerConfig().isEnablePopLog()) {
                                POP_LOGGER.info("timeout , wakeUp polling : {}", first);
                            }
                            totalPollingNum.decrementAndGet();
                            wakeUp(first);
                        }
                        while (true);
                        if (i >= 100) {
                            long tmpPollingNum = popQ.size();
                            tmpTotalPollingNum = tmpTotalPollingNum + tmpPollingNum;
                            if (tmpPollingNum > 100) {
                                POP_LOGGER.info("polling queue {} , size={} ", key, tmpPollingNum);
                            }
                        }
                    }

                    if (i >= 100) {
                        POP_LOGGER.info("pollingMapSize={},tmpTotalSize={},atomicTotalSize={},diffSize={}",
                            pollingMap.size(), tmpTotalPollingNum, totalPollingNum.get(),
                            Math.abs(totalPollingNum.get() - tmpTotalPollingNum));
                        totalPollingNum.set(tmpTotalPollingNum);
                        i = 0;
                    }

                    // clean unused
                    if (lastCleanTime == 0 || System.currentTimeMillis() - lastCleanTime > 5 * 60 * 1000) {
                        cleanUnusedResource();
                    }
                } catch (Throwable e) {
                    POP_LOGGER.error("checkPolling error", e);
                }
            }
            // clean all;
            try {
                Iterator<Entry<String, ConcurrentSkipListSet<PopRequest>>> pollingMapIterator = pollingMap.entrySet().iterator();
                while (pollingMapIterator.hasNext()) {
                    Entry<String, ConcurrentSkipListSet<PopRequest>> entry = pollingMapIterator.next();
                    ConcurrentSkipListSet<PopRequest> popQ = entry.getValue();
                    PopRequest first;
                    while ((first = popQ.pollFirst()) != null) {
                        wakeUp(first);
                    }
                }
            } catch (Throwable e) {
            }
        }
    }

    static class TimedLock {
        private final AtomicBoolean lock;
        private volatile long lockTime;

        public TimedLock() {
            this.lock = new AtomicBoolean(true);
            this.lockTime = System.currentTimeMillis();
        }

        public boolean tryLock() {
            boolean ret = lock.compareAndSet(true, false);
            if (ret) {
                this.lockTime = System.currentTimeMillis();
                return true;
            } else {
                return false;
            }
        }

        public void unLock() {
            lock.set(true);
        }

        public boolean isLock() {
            return !lock.get();
        }

        public long getLockTime() {
            return lockTime;
        }
    }

    /**
     * POP 消费队列锁管理器，服务线程
     */
    public class QueueLockManager extends ServiceThread {
        private ConcurrentHashMap<String, TimedLock> expiredLocalCache = new ConcurrentHashMap<>(100000);

        public String buildLockKey(String topic, String consumerGroup, int queueId) {
            return topic + PopAckConstants.SPLIT + consumerGroup + PopAckConstants.SPLIT + queueId;
        }

        public boolean tryLock(String topic, String consumerGroup, int queueId) {
            return tryLock(buildLockKey(topic, consumerGroup, queueId));
        }

        public boolean tryLock(String key) {
            TimedLock timedLock = expiredLocalCache.get(key);

            if (timedLock == null) {
                TimedLock old = expiredLocalCache.putIfAbsent(key, new TimedLock());
                if (old != null) {
                    return false;
                } else {
                    timedLock = expiredLocalCache.get(key);
                }
            }

            if (timedLock == null) {
                return false;
            }

            return timedLock.tryLock();
        }

        /**
         * is not thread safe, may cause duplicate lock
         *
         * @param usedExpireMillis
         * @return
         */
        public int cleanUnusedLock(final long usedExpireMillis) {
            Iterator<Entry<String, TimedLock>> iterator = expiredLocalCache.entrySet().iterator();

            int total = 0;
            while (iterator.hasNext()) {
                Entry<String, TimedLock> entry = iterator.next();

                if (System.currentTimeMillis() - entry.getValue().getLockTime() > usedExpireMillis) {
                    iterator.remove();
                    POP_LOGGER.info("Remove unused queue lock: {}, {}, {}", entry.getKey(),
                        entry.getValue().getLockTime(),
                        entry.getValue().isLock());
                }

                total++;
            }

            return total;
        }

        public void unLock(String topic, String consumerGroup, int queueId) {
            unLock(buildLockKey(topic, consumerGroup, queueId));
        }

        public void unLock(String key) {
            TimedLock timedLock = expiredLocalCache.get(key);
            if (timedLock != null) {
                timedLock.unLock();
            }
        }

        @Override
        public String getServiceName() {
            if (PopMessageProcessor.this.brokerController.getBrokerConfig().isInBrokerContainer()) {
                return PopMessageProcessor.this.brokerController.getBrokerIdentity().getIdentifier() + QueueLockManager.class.getSimpleName();
            }
            return QueueLockManager.class.getSimpleName();
        }

        @Override
        public void run() {
            while (!isStopped()) {
                try {
                    this.waitForRunning(60000);
                    int count = cleanUnusedLock(60000);
                    POP_LOGGER.info("QueueLockSize={}", count);
                } catch (Exception e) {
                    PopMessageProcessor.POP_LOGGER.error("QueueLockManager run error", e);
                }
            }
        }
    }
}
