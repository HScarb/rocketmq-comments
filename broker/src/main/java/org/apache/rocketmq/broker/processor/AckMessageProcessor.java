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

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.metrics.PopMetricsManager;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.pop.AckMsg;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

/**
 * POP 消费 ACK 处理
 */
public class AckMessageProcessor implements NettyRequestProcessor {
    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    private final BrokerController brokerController;
    private String reviveTopic;
    private PopReviveService[] popReviveServices;

    public AckMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.reviveTopic = PopAckConstants.buildClusterReviveTopic(this.brokerController.getBrokerConfig().getBrokerClusterName());
        // 为每个 Revive 队列创建一个 Revive 处理线程，默认 8 个
        this.popReviveServices = new PopReviveService[this.brokerController.getBrokerConfig().getReviveQueueNum()];
        for (int i = 0; i < this.brokerController.getBrokerConfig().getReviveQueueNum(); i++) {
            this.popReviveServices[i] = new PopReviveService(brokerController, reviveTopic, i);
            this.popReviveServices[i].setShouldRunPopRevive(brokerController.getBrokerConfig().getBrokerId() == 0);
        }
    }

    public PopReviveService[] getPopReviveServices() {
        return popReviveServices;
    }

    public void startPopReviveService() {
        for (PopReviveService popReviveService : popReviveServices) {
            popReviveService.start();
        }
    }

    public void shutdownPopReviveService() {
        for (PopReviveService popReviveService : popReviveServices) {
            popReviveService.stop();
        }
    }

    public void setPopReviveServiceStatus(boolean shouldStart) {
        for (PopReviveService popReviveService : popReviveServices) {
            popReviveService.setShouldRunPopRevive(shouldStart);
        }
    }

    public boolean isPopReviveServiceRunning() {
        for (PopReviveService popReviveService : popReviveServices) {
            if (popReviveService.isShouldRunPopRevive()) {
                return true;
            }
        }

        return false;
    }

    /**
     * 处理 Pop 消息 Ack 请求
     *
     * @param ctx
     * @param request
     * @return
     * @throws RemotingCommandException
     */
    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        return this.processRequest(ctx.channel(), request, true);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    /**
     * 处理 Ack 消息请求，每次 Ack 一条消息
     *
     * @param channel
     * @param request
     * @param brokerAllowSuspend
     * @return
     * @throws RemotingCommandException
     */
    private RemotingCommand processRequest(final Channel channel, RemotingCommand request,
        boolean brokerAllowSuspend) throws RemotingCommandException {
        // 解析请求头
        final AckMessageRequestHeader requestHeader = (AckMessageRequestHeader) request.decodeCommandCustomHeader(AckMessageRequestHeader.class);
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        AckMsg ackMsg = new AckMsg();
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        response.setOpaque(request.getOpaque());
        // 校验
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            POP_LOGGER.error("The topic {} not exist, consumer: {} ", requestHeader.getTopic(), RemotingHelper.parseChannelRemoteAddr(channel));
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(String.format("topic[%s] not exist, apply first please! %s", requestHeader.getTopic(), FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL)));
            return response;
        }

        if (requestHeader.getQueueId() >= topicConfig.getReadQueueNums() || requestHeader.getQueueId() < 0) {
            String errorInfo = String.format("queueId[%d] is illegal, topic:[%s] topicConfig.readQueueNums:[%d] consumer:[%s]",
                requestHeader.getQueueId(), requestHeader.getTopic(), topicConfig.getReadQueueNums(), channel.remoteAddress());
            POP_LOGGER.warn(errorInfo);
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            response.setRemark(errorInfo);
            return response;
        }
        long minOffset = this.brokerController.getMessageStore().getMinOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId());
        long maxOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId());
        if (requestHeader.getOffset() < minOffset || requestHeader.getOffset() > maxOffset) {
            String errorInfo = String.format("offset is illegal, key:%s@%d, commit:%d, store:%d~%d",
                requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getOffset(), minOffset, maxOffset);
            POP_LOGGER.warn(errorInfo);
            response.setCode(ResponseCode.NO_MESSAGE);
            response.setRemark(errorInfo);
            return response;
        }
        // 拆分消息句柄字符串
        String[] extraInfo = ExtraInfoUtil.split(requestHeader.getExtraInfo());

        // 用请求头中的信息构造 AckMsg
        ackMsg.setAckOffset(requestHeader.getOffset());
        ackMsg.setStartOffset(ExtraInfoUtil.getCkQueueOffset(extraInfo));
        ackMsg.setConsumerGroup(requestHeader.getConsumerGroup());
        ackMsg.setTopic(requestHeader.getTopic());
        ackMsg.setQueueId(requestHeader.getQueueId());
        ackMsg.setPopTime(ExtraInfoUtil.getPopTime(extraInfo));
        ackMsg.setBrokerName(ExtraInfoUtil.getBrokerName(extraInfo));

        int rqId = ExtraInfoUtil.getReviveQid(extraInfo);
        long invisibleTime = ExtraInfoUtil.getInvisibleTime(extraInfo);

        this.brokerController.getBrokerStatsManager().incBrokerAckNums(1);
        this.brokerController.getBrokerStatsManager().incGroupAckNums(requestHeader.getConsumerGroup(), requestHeader.getTopic(), 1);

        if (rqId == KeyBuilder.POP_ORDER_REVIVE_QUEUE) {
            // 顺序消息 ACK
            // order
            String lockKey = requestHeader.getTopic() + PopAckConstants.SPLIT
                + requestHeader.getConsumerGroup() + PopAckConstants.SPLIT + requestHeader.getQueueId();
            long oldOffset = this.brokerController.getConsumerOffsetManager().queryOffset(requestHeader.getConsumerGroup(),
                requestHeader.getTopic(), requestHeader.getQueueId());
            if (requestHeader.getOffset() < oldOffset) {
                return response;
            }
            while (!this.brokerController.getPopMessageProcessor().getQueueLockManager().tryLock(lockKey)) {
            }
            try {
                oldOffset = this.brokerController.getConsumerOffsetManager().queryOffset(requestHeader.getConsumerGroup(),
                    requestHeader.getTopic(), requestHeader.getQueueId());
                if (requestHeader.getOffset() < oldOffset) {
                    return response;
                }
                long nextOffset = brokerController.getConsumerOrderInfoManager().commitAndNext(
                    requestHeader.getTopic(), requestHeader.getConsumerGroup(),
                    requestHeader.getQueueId(), requestHeader.getOffset(),
                    ExtraInfoUtil.getPopTime(extraInfo));
                if (nextOffset > -1) {
                    if (!this.brokerController.getConsumerOffsetManager().hasOffsetReset(
                        requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getQueueId())) {
                        this.brokerController.getConsumerOffsetManager().commitOffset(channel.remoteAddress().toString(),
                            requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId(), nextOffset);
                    }
                    if (!this.brokerController.getConsumerOrderInfoManager().checkBlock(requestHeader.getTopic(),
                        requestHeader.getConsumerGroup(), requestHeader.getQueueId(), invisibleTime)) {
                        this.brokerController.getPopMessageProcessor().notifyMessageArriving(
                            requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getQueueId());
                    }
                } else if (nextOffset == -1) {
                    String errorInfo = String.format("offset is illegal, key:%s, old:%d, commit:%d, next:%d, %s",
                        lockKey, oldOffset, requestHeader.getOffset(), nextOffset, channel.remoteAddress());
                    POP_LOGGER.warn(errorInfo);
                    response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                    response.setRemark(errorInfo);
                    return response;
                }
            } finally {
                this.brokerController.getPopMessageProcessor().getQueueLockManager().unLock(lockKey);
            }
            decInFlightMessageNum(requestHeader);
            return response;
        }

        // 普通消息 ACK
        // 先尝试放入内存匹配，成功则直接返回。失败可能是内存匹配未开启
        if (this.brokerController.getPopMessageProcessor().getPopBufferMergeService().addAk(rqId, ackMsg)) {
            decInFlightMessageNum(requestHeader);
            return response;
        }

        // 构造 Ack 消息
        msgInner.setTopic(reviveTopic);
        msgInner.setBody(JSON.toJSONString(ackMsg).getBytes(DataConverter.charset));
        //msgInner.setQueueId(Integer.valueOf(extraInfo[3]));
        msgInner.setQueueId(rqId);
        msgInner.setTags(PopAckConstants.ACK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(this.brokerController.getStoreHost());
        msgInner.setStoreHost(this.brokerController.getStoreHost());
        // 定时消息，定时到唤醒重试时间投递
        msgInner.setDeliverTimeMs(ExtraInfoUtil.getPopTime(extraInfo) + invisibleTime);
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, PopMessageProcessor.genAckUniqueId(ackMsg));
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        // 保存 Ack 消息到磁盘
        PutMessageResult putMessageResult = this.brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);
        if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
            POP_LOGGER.error("put ack msg error:" + putMessageResult);
        }
        PopMetricsManager.incPopReviveAckPutCount(ackMsg, putMessageResult.getPutMessageStatus());
        decInFlightMessageNum(requestHeader);
        return response;
    }

    private void decInFlightMessageNum(AckMessageRequestHeader requestHeader) {
        this.brokerController.getPopInflightMessageCounter().decrementInFlightMessageNum(
            requestHeader.getTopic(),
            requestHeader.getConsumerGroup(),
            requestHeader.getExtraInfo()
        );
    }

}
