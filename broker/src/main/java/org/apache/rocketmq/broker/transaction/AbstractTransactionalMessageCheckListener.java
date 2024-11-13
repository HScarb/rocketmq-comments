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
package org.apache.rocketmq.broker.transaction;

import io.netty.channel.Channel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;

/**
 * Broker 端事务消息状态回查基类，回查事务消息状态，执行提交或回滚
 */
public abstract class AbstractTransactionalMessageCheckListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private BrokerController brokerController;

    //queue nums of topic TRANS_CHECK_MAX_TIME_TOPIC
    protected final static int TCMT_QUEUE_NUMS = 1;

    /**
     * 事务半消息回查线程池
     */
    private static volatile ExecutorService executorService;

    public AbstractTransactionalMessageCheckListener() {
    }

    public AbstractTransactionalMessageCheckListener(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * 发送回查请求到生产者客户端
     *
     * @param msgExt 事务半消息
     * @throws Exception
     */
    public void sendCheckMessage(MessageExt msgExt) throws Exception {
        CheckTransactionStateRequestHeader checkTransactionStateRequestHeader = new CheckTransactionStateRequestHeader();
        checkTransactionStateRequestHeader.setTopic(msgExt.getTopic());
        checkTransactionStateRequestHeader.setCommitLogOffset(msgExt.getCommitLogOffset());
        checkTransactionStateRequestHeader.setOffsetMsgId(msgExt.getMsgId());
        checkTransactionStateRequestHeader.setMsgId(msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        checkTransactionStateRequestHeader.setTransactionId(checkTransactionStateRequestHeader.getMsgId());
        checkTransactionStateRequestHeader.setTranStateTableOffset(msgExt.getQueueOffset());
        checkTransactionStateRequestHeader.setBrokerName(brokerController.getBrokerConfig().getBrokerName());
        // 从消息属性中复原真实的 topic 和 queueId
        msgExt.setTopic(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC));
        msgExt.setQueueId(Integer.parseInt(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_QUEUE_ID)));
        msgExt.setStoreSize(0);
        String groupId = msgExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
        // 轮询选择一个可用的生产者客户端通道
        Channel channel = brokerController.getProducerManager().getAvailableChannel(groupId);
        if (channel != null) {
            brokerController.getBroker2Client().checkProducerTransactionState(groupId, channel, checkTransactionStateRequestHeader, msgExt);
        } else {
            LOGGER.warn("Check transaction failed, channel is null. groupId={}", groupId);
        }
    }

    /**
     * 处理事务半消息（回查其执行状态）
     * @param msgExt 事务半消息
     */
    public void resolveHalfMsg(final MessageExt msgExt) {
        if (executorService != null) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        sendCheckMessage(msgExt);
                    } catch (Exception e) {
                        LOGGER.error("Send check message error!", e);
                    }
                }
            });
        } else {
            LOGGER.error("TransactionalMessageCheckListener not init");
        }
    }

    public BrokerController getBrokerController() {
        return brokerController;
    }

    public void shutDown() {
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    public synchronized void initExecutorService() {
        if (executorService == null) {
            executorService = ThreadUtils.newThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<>(2000),
                new ThreadFactoryImpl("Transaction-msg-check-thread", brokerController.getBrokerIdentity()), new CallerRunsPolicy());
        }
    }

    /**
     * Inject brokerController for this listener
     *
     * @param brokerController
     */
    public void setBrokerController(BrokerController brokerController) {
        this.brokerController = brokerController;
        initExecutorService();
    }

    /**
     * 丢弃事务半消息
     * In order to avoid check back unlimited, we will discard the message that have been checked more than a certain
     * number of times.
     *
     * @param msgExt Message to be discarded.
     */
    public abstract void resolveDiscardMsg(MessageExt msgExt);
}
