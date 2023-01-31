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
package org.apache.rocketmq.store;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.CommitLog.MessageExtEncoder;
import org.apache.rocketmq.store.dledger.DLedgerCommitLog;

/**
 * not-thread-safe
 */
public class MultiDispatch {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final StringBuilder keyBuilder = new StringBuilder();
    private final DefaultMessageStore messageStore;
    private final CommitLog commitLog;
    private boolean isDLedger;

    public MultiDispatch(DefaultMessageStore messageStore, CommitLog commitLog) {
        this.messageStore = messageStore;
        this.commitLog = commitLog;
        isDLedger = commitLog instanceof DLedgerCommitLog;
    }

    /**
     * 消息是否需要多队列分发
     *
     * @param msg
     * @return
     */
    public boolean isMultiDispatchMsg(MessageExtBrokerInner msg) {
        if (!messageStore.getMessageStoreConfig().isEnableMultiDispatch()) {
            return false;
        }
        if (StringUtils.isBlank(msg.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH))) {
            return false;
        }
        return true;
    }

    /**
     * 构造队列 Key
     *
     * @param queueName 轻量级队列名称
     * @param msgInner 原始消息
     * @return "队列名称-ID"
     */
    public String queueKey(String queueName, MessageExtBrokerInner msgInner) {
        keyBuilder.setLength(0);
        keyBuilder.append(queueName);
        keyBuilder.append('-');
        int queueId = msgInner.getQueueId();
        if (messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(queueName)) {
            queueId = 0;
        }
        keyBuilder.append(queueId);
        return keyBuilder.toString();
    }

    /**
     * 为消息添加多队列分发属性
     *
     * @param msgInner
     * @return
     */
    public boolean wrapMultiDispatch(final MessageExtBrokerInner msgInner) {
        if (!messageStore.getMessageStoreConfig().isEnableMultiDispatch()) {
            return true;
        }
        String multiDispatchQueue = msgInner.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        if (StringUtils.isBlank(multiDispatchQueue)) {
            return true;
        }
        // 从原始消息属性中获取分发的队列列表
        String[] queues = multiDispatchQueue.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
        // 从队列偏移量表中查询队列当前偏移量
        Long[] queueOffsets = new Long[queues.length];
        for (int i = 0; i < queues.length; i++) {
            String key = queueKey(queues[i], msgInner);
            Long queueOffset;
            try {
                queueOffset = getTopicQueueOffset(key);
            } catch (Exception e) {
                return false;
            }
            if (null == queueOffset) {
                queueOffset = 0L;
                if (messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(key)) {
                    commitLog.getLmqTopicQueueTable().put(key, queueOffset);
                } else {
                    commitLog.getTopicQueueTable().put(key, queueOffset);
                }
            }
            queueOffsets[i] = queueOffset;
        }
        // 将队列偏移量作为属性存入消息
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET,
            StringUtils.join(queueOffsets, MixAll.MULTI_DISPATCH_QUEUE_SPLITTER));
        // 移除消息的 WAIT_STORE 属性，节省存储空间
        removeWaitStorePropertyString(msgInner);
        if (isDLedger) {
            return true;
        } else {
            return rebuildMsgInner(msgInner);
        }
    }

    /**
     * 移除消息的 WAIT_STORE 属性，节省空间
     *
     * @param msgInner
     */
    private void removeWaitStorePropertyString(MessageExtBrokerInner msgInner) {
        if (msgInner.getProperties().containsKey(MessageConst.PROPERTY_WAIT_STORE_MSG_OK)) {
            // There is no need to store "WAIT=true", remove it from propertiesString to save 9 bytes for each message.
            // It works for most case. In some cases msgInner.setPropertiesString invoked later and replace it.
            String waitStoreMsgOKValue = msgInner.getProperties().remove(MessageConst.PROPERTY_WAIT_STORE_MSG_OK);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
            // Reput to properties, since msgInner.isWaitStoreMsgOK() will be invoked later
            msgInner.getProperties().put(MessageConst.PROPERTY_WAIT_STORE_MSG_OK, waitStoreMsgOKValue);
        } else {
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        }
    }

    public void updateMaxMessageSize(CommitLog.PutMessageThreadLocal putMessageThreadLocal) {
        int newMaxMessageSize = this.messageStore.getMessageStoreConfig().getMaxMessageSize();
        if (newMaxMessageSize >= 10 &&
                putMessageThreadLocal.getEncoder().getMaxMessageBodySize() != newMaxMessageSize) {
            putMessageThreadLocal.getEncoder().updateEncoderBufferCapacity(newMaxMessageSize);
        }
    }

    private boolean rebuildMsgInner(MessageExtBrokerInner msgInner) {
        CommitLog.PutMessageThreadLocal putMessageThreadLocal = this.commitLog.getPutMessageThreadLocal().get();
        updateMaxMessageSize(putMessageThreadLocal);
        MessageExtEncoder encoder = putMessageThreadLocal.getEncoder();
        PutMessageResult encodeResult = encoder.encode(msgInner);
        if (encodeResult != null) {
            LOGGER.error("rebuild msgInner for multiDispatch", encodeResult);
            return false;
        }
        msgInner.setEncodedBuff(encoder.getEncoderBuffer());
        return true;

    }

    /**
     * 事务消息确认后更新队列偏移量
     *
     * @param msgInner
     */
    public void updateMultiQueueOffset(final MessageExtBrokerInner msgInner) {
        if (!messageStore.getMessageStoreConfig().isEnableMultiDispatch()) {
            return;
        }
        String multiDispatchQueue = msgInner.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        if (StringUtils.isBlank(multiDispatchQueue)) {
            return;
        }
        String multiQueueOffset = msgInner.getProperty(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET);
        if (StringUtils.isBlank(multiQueueOffset)) {
            LOGGER.error("[bug] no multiQueueOffset when updating {}", msgInner.getTopic());
            return;
        }
        String[] queues = multiDispatchQueue.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
        String[] queueOffsets = multiQueueOffset.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
        if (queues.length != queueOffsets.length) {
            LOGGER.error("[bug] num is not equal when updateMultiQueueOffset {}", msgInner.getTopic());
            return;
        }
        for (int i = 0; i < queues.length; i++) {
            String key = queueKey(queues[i], msgInner);
            long queueOffset = Long.parseLong(queueOffsets[i]);
            if (messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(key)) {
                commitLog.getLmqTopicQueueTable().put(key, ++queueOffset);
            } else {
                commitLog.getTopicQueueTable().put(key, ++queueOffset);
            }
        }
    }

    /**
     * 获取队列当前的逻辑偏移量
     *
     * @param key topic-queue
     * @return
     * @throws Exception
     */
    private Long getTopicQueueOffset(String key) throws Exception {
        Long offset = null;
        if (messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(key)) {
            Long queueNextOffset = commitLog.getLmqTopicQueueTable().get(key);
            if (queueNextOffset != null) {
                offset = queueNextOffset;
            }
        } else {
            offset = commitLog.getTopicQueueTable().get(key);
        }
        return offset;
    }

}
