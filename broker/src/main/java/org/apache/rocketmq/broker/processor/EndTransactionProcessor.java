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

import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.broker.transaction.OperationResult;
import org.apache.rocketmq.broker.transaction.queue.TransactionalMessageUtil;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.BrokerRole;

import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_TOPIC;

/**
 * 事务消息结果请求处理器，处理事务消息生产者发送的事务消息提交或回退请求
 * EndTransaction processor: process commit and rollback message
 */
public class EndTransactionProcessor implements NettyRequestProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);
    private final BrokerController brokerController;

    public EndTransactionProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * 两个场景会发触发 END_TRANSACTION 请求
     * 1. 生产者第一次发送事务消息，执行完本地事务后
     * 2. 生产者收到服务端事务回查请求，查询完本地事务执行状态
     */
    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws
        RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final EndTransactionRequestHeader requestHeader =
            (EndTransactionRequestHeader) request.decodeCommandCustomHeader(EndTransactionRequestHeader.class);
        LOGGER.debug("Transaction request:{}", requestHeader);
        if (BrokerRole.SLAVE == brokerController.getMessageStoreConfig().getBrokerRole()) {
            response.setCode(ResponseCode.SLAVE_NOT_AVAILABLE);
            LOGGER.warn("Message store is slave mode, so end transaction is forbidden. ");
            return response;
        }

        // 是否是客户端主动发起的请求，还是服务端回查后客户端发出的
        if (requestHeader.getFromTransactionCheck()) {
            // 服务端回查后客户端发出的请求
            switch (requestHeader.getCommitOrRollback()) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE: {
                    LOGGER.warn("Check producer[{}] transaction state, but it's pending status."
                            + "RequestHeader: {} Remark: {}",
                        RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                        requestHeader.toString(),
                        request.getRemark());
                    return null;
                }

                case MessageSysFlag.TRANSACTION_COMMIT_TYPE: {
                    LOGGER.warn("Check producer[{}] transaction state, the producer commit the message."
                            + "RequestHeader: {} Remark: {}",
                        RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                        requestHeader.toString(),
                        request.getRemark());

                    break;
                }

                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE: {
                    LOGGER.warn("Check producer[{}] transaction state, the producer rollback the message."
                            + "RequestHeader: {} Remark: {}",
                        RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                        requestHeader.toString(),
                        request.getRemark());
                    break;
                }
                default:
                    return null;
            }
        } else {
            // 客户端主动上报的事务消息提交或回滚请求
            switch (requestHeader.getCommitOrRollback()) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE: {
                    LOGGER.warn("The producer[{}] end transaction in sending message,  and it's pending status."
                            + "RequestHeader: {} Remark: {}",
                        RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                        requestHeader.toString(),
                        request.getRemark());
                    return null;
                }

                case MessageSysFlag.TRANSACTION_COMMIT_TYPE: {
                    break;
                }

                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE: {
                    LOGGER.warn("The producer[{}] end transaction in sending message, rollback the message."
                            + "RequestHeader: {} Remark: {}",
                        RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                        requestHeader.toString(),
                        request.getRemark());
                    break;
                }
                default:
                    return null;
            }
        }
        OperationResult result = new OperationResult();
        // 事务提交
        if (MessageSysFlag.TRANSACTION_COMMIT_TYPE == requestHeader.getCommitOrRollback()) {
            // 从存储中查询出事务半消息
            result = this.brokerController.getTransactionalMessageService().commitMessage(requestHeader);
            // 成功查询出事务半消息
            if (result.getResponseCode() == ResponseCode.SUCCESS) {
                if (rejectCommitOrRollback(requestHeader, result.getPrepareMessage())) {
                    response.setCode(ResponseCode.ILLEGAL_OPERATION);
                    LOGGER.warn("Message commit fail [producer end]. currentTimeMillis - bornTime > checkImmunityTime, msgId={},commitLogOffset={}, wait check",
                            requestHeader.getMsgId(), requestHeader.getCommitLogOffset());
                    return response;
                }
                // 验证事务半消息的必要字段
                RemotingCommand res = checkPrepareMessage(result.getPrepareMessage(), requestHeader);
                if (res.getCode() == ResponseCode.SUCCESS) {
                    // 验证成功
                    // 恢复事务半消息的真实 Topic、队列，并设置事务 ID，并设置相关属性
                    MessageExtBrokerInner msgInner = endMessageTransaction(result.getPrepareMessage());
                    msgInner.setSysFlag(MessageSysFlag.resetTransactionValue(msgInner.getSysFlag(), requestHeader.getCommitOrRollback()));
                    msgInner.setQueueOffset(requestHeader.getTranStateTableOffset());
                    msgInner.setPreparedTransactionOffset(requestHeader.getCommitLogOffset());
                    msgInner.setStoreTimestamp(result.getPrepareMessage().getStoreTimestamp());
                    MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_TRANSACTION_PREPARED);
                    // 发送最终的事务消息，存储到 CommitLog 中，可以被消费者消费
                    RemotingCommand sendResult = sendFinalMessage(msgInner);
                    if (sendResult.getCode() == ResponseCode.SUCCESS) {
                        // 删除事务半消息：将事务半消息存储在事务消息操作 Topic：RMQ_SYS_TRANS_OP_HALF_TOPIC 中，表示该消息已经被处理
                        this.brokerController.getTransactionalMessageService().deletePrepareMessage(result.getPrepareMessage());
                        // successful committed, then total num of half-messages minus 1
                        this.brokerController.getTransactionalMessageService().getTransactionMetrics().addAndGet(msgInner.getTopic(), -1);
                        BrokerMetricsManager.commitMessagesTotal.add(1, BrokerMetricsManager.newAttributesBuilder()
                                .put(LABEL_TOPIC, msgInner.getTopic())
                                .build());
                        // record the commit latency.
                        Long commitLatency = (System.currentTimeMillis() - result.getPrepareMessage().getBornTimestamp()) / 1000;
                        BrokerMetricsManager.transactionFinishLatency.record(commitLatency, BrokerMetricsManager.newAttributesBuilder()
                                .put(LABEL_TOPIC, msgInner.getTopic())
                                .build());
                    }
                    return sendResult;
                }
                return res;
            }
        // 事务回滚
        } else if (MessageSysFlag.TRANSACTION_ROLLBACK_TYPE == requestHeader.getCommitOrRollback()) {
            // 从存储中查询出事务半消息
            result = this.brokerController.getTransactionalMessageService().rollbackMessage(requestHeader);
            if (result.getResponseCode() == ResponseCode.SUCCESS) {
                // 成功查询出事务半消息
                if (rejectCommitOrRollback(requestHeader, result.getPrepareMessage())) {
                    response.setCode(ResponseCode.ILLEGAL_OPERATION);
                    LOGGER.warn("Message rollback fail [producer end]. currentTimeMillis - bornTime > checkImmunityTime, msgId={},commitLogOffset={}, wait check",
                            requestHeader.getMsgId(), requestHeader.getCommitLogOffset());
                    return response;
                }
                RemotingCommand res = checkPrepareMessage(result.getPrepareMessage(), requestHeader);
                if (res.getCode() == ResponseCode.SUCCESS) {
                    // 删除事务半消息：将事务半消息存储在事务消息操作 Topic：RMQ_SYS_TRANS_OP_HALF_TOPIC 中，表示该消息已经被处理
                    this.brokerController.getTransactionalMessageService().deletePrepareMessage(result.getPrepareMessage());
                    // roll back, then total num of half-messages minus 1
                    this.brokerController.getTransactionalMessageService().getTransactionMetrics().addAndGet(result.getPrepareMessage().getProperty(MessageConst.PROPERTY_REAL_TOPIC), -1);
                    BrokerMetricsManager.rollBackMessagesTotal.add(1, BrokerMetricsManager.newAttributesBuilder()
                            .put(LABEL_TOPIC, result.getPrepareMessage().getProperty(MessageConst.PROPERTY_REAL_TOPIC))
                            .build());
                }
                return res;
            }
        }
        // 事务执行状态未知则不做处理
        response.setCode(result.getResponseCode());
        response.setRemark(result.getResponseRemark());
        return response;
    }

    /**
     * If you specify a custom first check time CheckImmunityTimeInSeconds,
     * And the commit/rollback request whose validity period exceeds CheckImmunityTimeInSeconds and is not checked back will be processed and failed
     * returns ILLEGAL_OPERATION 604 error
     * @param requestHeader
     * @param messageExt
     * @return
     */
    public boolean rejectCommitOrRollback(EndTransactionRequestHeader requestHeader, MessageExt messageExt) {
        if (requestHeader.getFromTransactionCheck()) {
            return false;
        }
        long transactionTimeout = brokerController.getBrokerConfig().getTransactionTimeOut();

        String checkImmunityTimeStr = messageExt.getUserProperty(MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS);
        if (StringUtils.isNotEmpty(checkImmunityTimeStr)) {
            long valueOfCurrentMinusBorn = System.currentTimeMillis() - messageExt.getBornTimestamp();
            long checkImmunityTime = TransactionalMessageUtil.getImmunityTime(checkImmunityTimeStr, transactionTimeout);
            //Non-check requests that exceed the specified custom first check time fail to return
            return valueOfCurrentMinusBorn > checkImmunityTime;
        }
        return false;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    /**
     * 验证事务半消息的必要字段
     * @param msgExt 从存储中查询出来的事务半消息
     * @param requestHeader 事务提交或回滚的请求头
     * @return 返回的验证结果
     */
    private RemotingCommand checkPrepareMessage(MessageExt msgExt, EndTransactionRequestHeader requestHeader) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        if (msgExt != null) {
            // 验证消息的生产组与请求信息中的生产组是否一致
            final String pgroupRead = msgExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
            if (!pgroupRead.equals(requestHeader.getProducerGroup())) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("The producer group wrong");
                return response;
            }

            // 验证消息的队列偏移量与请求信息中的偏移量是否一致
            if (msgExt.getQueueOffset() != requestHeader.getTranStateTableOffset()) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("The transaction state table offset wrong");
                return response;
            }

            // 验证消息的物理偏移量与请求信息中的物理偏移量是否一致
            if (msgExt.getCommitLogOffset() != requestHeader.getCommitLogOffset()) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("The commit log offset wrong");
                return response;
            }
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("Find prepared transaction message failed");
            return response;
        }
        response.setCode(ResponseCode.SUCCESS);
        return response;
    }

    /**
     * 恢复事务半消息的真实 Topic、队列，并设置事务 ID
     * @param msgExt 第一次发送并存储在 Broker 的事务半消息
     * @return 还原后的事务消息
     */
    private MessageExtBrokerInner endMessageTransaction(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC));
        msgInner.setQueueId(Integer.parseInt(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_QUEUE_ID)));
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());
        msgInner.setWaitStoreMsgOK(false);
        msgInner.setTransactionId(msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        msgInner.setSysFlag(msgExt.getSysFlag());
        TopicFilterType topicFilterType =
            (msgInner.getSysFlag() & MessageSysFlag.MULTI_TAGS_FLAG) == MessageSysFlag.MULTI_TAGS_FLAG ? TopicFilterType.MULTI_TAG
                : TopicFilterType.SINGLE_TAG;
        long tagsCodeValue = MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_TOPIC);
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_QUEUE_ID);
        return msgInner;
    }

    /**
     * 发送最终的事务消息
     * @param msgInner 还原后的事务消息
     * @return 返回结果
     */
    private RemotingCommand sendFinalMessage(MessageExtBrokerInner msgInner) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        if (putMessageResult != null) {
            switch (putMessageResult.getPutMessageStatus()) {
                // Success
                case PUT_OK:
                    this.brokerController.getBrokerStatsManager().incTopicPutNums(msgInner.getTopic(), putMessageResult.getAppendMessageResult().getMsgNum(), 1);
                    this.brokerController.getBrokerStatsManager().incTopicPutSize(msgInner.getTopic(), putMessageResult.getAppendMessageResult().getWroteBytes());
                    this.brokerController.getBrokerStatsManager().incBrokerPutNums(msgInner.getTopic(), putMessageResult.getAppendMessageResult().getMsgNum());
                case FLUSH_DISK_TIMEOUT:
                case FLUSH_SLAVE_TIMEOUT:
                case SLAVE_NOT_AVAILABLE:
                    response.setCode(ResponseCode.SUCCESS);
                    response.setRemark(null);
                    break;
                // Failed
                case CREATE_MAPPED_FILE_FAILED:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("Create mapped file failed.");
                    break;
                case MESSAGE_ILLEGAL:
                case PROPERTIES_SIZE_EXCEEDED:
                    response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                    response.setRemark(String.format("The message is illegal, maybe msg body or properties length not matched. msg body length limit %dB, msg properties length limit 32KB.",
                        this.brokerController.getMessageStoreConfig().getMaxMessageSize()));
                    break;
                case SERVICE_NOT_AVAILABLE:
                    response.setCode(ResponseCode.SERVICE_NOT_AVAILABLE);
                    response.setRemark("Service not available now.");
                    break;
                case OS_PAGE_CACHE_BUSY:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("OS page cache busy, please try another machine");
                    break;
                case WHEEL_TIMER_MSG_ILLEGAL:
                    response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                    response.setRemark(String.format("timer message illegal, the delay time should not be bigger than the max delay %dms; or if set del msg, the delay time should be bigger than the current time",
                        this.brokerController.getMessageStoreConfig().getTimerMaxDelaySec() * 1000L));
                    break;
                case WHEEL_TIMER_FLOW_CONTROL:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark(String.format("timer message is under flow control, max num limit is %d or the current value is greater than %d and less than %d, trigger random flow control",
                        this.brokerController.getMessageStoreConfig().getTimerCongestNumEachSlot() * 2L, this.brokerController.getMessageStoreConfig().getTimerCongestNumEachSlot(), this.brokerController.getMessageStoreConfig().getTimerCongestNumEachSlot() * 2L));
                    break;
                case WHEEL_TIMER_NOT_ENABLE:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark(String.format("accurate timer message is not enabled, timerWheelEnable is %s",
                        this.brokerController.getMessageStoreConfig().isTimerWheelEnable()));
                    break;
                case UNKNOWN_ERROR:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("UNKNOWN_ERROR");
                    break;
                case IN_SYNC_REPLICAS_NOT_ENOUGH:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("in-sync replicas not enough");
                    break;
                case PUT_TO_REMOTE_BROKER_FAIL:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("put to remote broker fail");
                    break;
                default:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("UNKNOWN_ERROR DEFAULT");
                    break;
            }
            return response;
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("store putMessage return null");
        }
        return response;
    }
}
