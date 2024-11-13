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
package org.apache.rocketmq.broker.transaction.queue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.OperationResult;
import org.apache.rocketmq.broker.transaction.TransactionMetrics;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.BrokerRole;

public class TransactionalMessageServiceImpl implements TransactionalMessageService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private TransactionalMessageBridge transactionalMessageBridge;

    private static final int PULL_MSG_RETRY_NUMBER = 1;

    /**
     * 单个队列消息回查过程的最大持续时间，默认 1 分钟
     */
    private static final int MAX_PROCESS_TIME_LIMIT = 60000;
    private static final int MAX_RETRY_TIMES_FOR_ESCAPE = 10;

    private static final int MAX_RETRY_COUNT_WHEN_HALF_NULL = 1;

    /**
     * 事务操作消息单次拉取数量
     */
    private static final int OP_MSG_PULL_NUMS = 32;

    private static final int SLEEP_WHILE_NO_OP = 1000;

    /**
     * 每个事务半消息队列对应的事务操作消息批量提交上下文，默认情况下事务半消息队列和事务操作消息队列都只有 1 个。这里也作为内存缓存
     */
    private final ConcurrentHashMap<Integer, MessageQueueOpContext> deleteContext = new ConcurrentHashMap<>();

    private ServiceThread transactionalOpBatchService;

    private ConcurrentHashMap<MessageQueue, MessageQueue> opQueueMap = new ConcurrentHashMap<>();

    private TransactionMetrics transactionMetrics;

    public TransactionalMessageServiceImpl(TransactionalMessageBridge transactionBridge) {
        this.transactionalMessageBridge = transactionBridge;
        transactionalOpBatchService = new TransactionalOpBatchService(transactionalMessageBridge.getBrokerController(), this);
        transactionalOpBatchService.start();
        transactionMetrics = new TransactionMetrics(BrokerPathConfigHelper.getTransactionMetricsPath(
                transactionalMessageBridge.getBrokerController().getMessageStoreConfig().getStorePathRootDir()));
        transactionMetrics.load();
    }

    @Override
    public TransactionMetrics getTransactionMetrics() {
        return transactionMetrics;
    }

    @Override
    public void setTransactionMetrics(TransactionMetrics transactionMetrics) {
        this.transactionMetrics = transactionMetrics;
    }


    @Override
    public CompletableFuture<PutMessageResult> asyncPrepareMessage(MessageExtBrokerInner messageInner) {
        return transactionalMessageBridge.asyncPutHalfMessage(messageInner);
    }

    @Override
    public PutMessageResult prepareMessage(MessageExtBrokerInner messageInner) {
        return transactionalMessageBridge.putHalfMessage(messageInner);
    }

    /**
     * 判断事务消息是否需要丢弃，即超过最大回查次数，默认 15 次。每次回查都会将消息属性中的回查次数 + 1，并重新保存事务半消息
     * @param msgExt 事务半消息
     * @param transactionCheckMax 最大回查次数
     * @return 是否需要丢弃
     */
    private boolean needDiscard(MessageExt msgExt, int transactionCheckMax) {
        String checkTimes = msgExt.getProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES);
        int checkTime = 1;
        if (null != checkTimes) {
            checkTime = getInt(checkTimes);
            if (checkTime >= transactionCheckMax) {
                return true;
            } else {
                checkTime++;
            }
        }
        // 更新消息属性中的事务回查次数
        msgExt.putUserProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES, String.valueOf(checkTime));
        return false;
    }

    /**
     * 是否超过了文件保存时间，默认 72 小时
     * @param msgExt 事务半消息
     * @return
     */
    private boolean needSkip(MessageExt msgExt) {
        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();
        if (valueOfCurrentMinusBorn
            > transactionalMessageBridge.getBrokerController().getMessageStoreConfig().getFileReservedTime()
            * 3600L * 1000) {
            log.info("Half message exceed file reserved time ,so skip it.messageId {},bornTime {}",
                msgExt.getMsgId(), msgExt.getBornTimestamp());
            return true;
        }
        return false;
    }

    private boolean putBackHalfMsgQueue(MessageExt msgExt, long offset) {
        PutMessageResult putMessageResult = putBackToHalfQueueReturnResult(msgExt);
        if (putMessageResult != null
            && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
            msgExt.setQueueOffset(
                putMessageResult.getAppendMessageResult().getLogicsOffset());
            msgExt.setCommitLogOffset(
                putMessageResult.getAppendMessageResult().getWroteOffset());
            msgExt.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
            log.debug(
                "Send check message, the offset={} restored in queueOffset={} "
                    + "commitLogOffset={} "
                    + "newMsgId={} realMsgId={} topic={}",
                offset, msgExt.getQueueOffset(), msgExt.getCommitLogOffset(), msgExt.getMsgId(),
                msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                msgExt.getTopic());
            return true;
        } else {
            log.error(
                "PutBackToHalfQueueReturnResult write failed, topic: {}, queueId: {}, "
                    + "msgId: {}",
                msgExt.getTopic(), msgExt.getQueueId(), msgExt.getMsgId());
            return false;
        }
    }

    /**
     * 发送请求到生产者客户端，回查事务消息执行状态
     *
     * @param transactionTimeout The minimum time of the transactional message to be checked firstly, one message only
     * exceed this time interval that can be checked.
     *        事务消息首次回查的最小时间，超过这个时间间隔的事务消息才会被回查
     * @param transactionCheckMax The maximum number of times the message was checked, if exceed this value, this
     * message will be discarded.
     *        消息被回查的最大次数，超过这个次数则丢弃
     * @param listener When the message is considered to be checked or discarded, the relative method of this class will
     * be invoked.
     *        当消息被回查或者丢弃时，会调用该监听器的方法
     */
    @Override
    public void check(long transactionTimeout, int transactionCheckMax,
        AbstractTransactionalMessageCheckListener listener) {
        try {
            String topic = TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC;
            // 获取事务半消息 Topic 的所有队列，默认只有 1 个队列
            Set<MessageQueue> msgQueues = transactionalMessageBridge.fetchMessageQueues(topic);
            if (msgQueues == null || msgQueues.size() == 0) {
                log.warn("The queue of topic is empty :" + topic);
                return;
            }
            log.debug("Check topic={}, queues={}", topic, msgQueues);
            for (MessageQueue messageQueue : msgQueues) {
                long startTime = System.currentTimeMillis();
                // 获取半消息队列对应的操作队列，主题为：RMQ_SYS_TRANS_OP_HALF_TOPIC。该主题保存事务消息提交或者回滚的请求
                MessageQueue opQueue = getOpQueue(messageQueue);
                // 内部消费组 CID_SYS_RMQ_TRANS 对事务半消息的消费进度
                long halfOffset = transactionalMessageBridge.fetchConsumeOffset(messageQueue);
                // 内部消费组 CID_SYS_RMQ_TRANS 对操作队列的消费进度
                long opOffset = transactionalMessageBridge.fetchConsumeOffset(opQueue);
                log.info("Before check, the queue={} msgOffset={} opOffset={}", messageQueue, halfOffset, opOffset);
                if (halfOffset < 0 || opOffset < 0) {
                    log.error("MessageQueue: {} illegal offset read: {}, op offset: {},skip this queue", messageQueue,
                        halfOffset, opOffset);
                    continue;
                }

                // 已经处理完的操作消息的偏移量列表
                List<Long> doneOpOffset = new ArrayList<>();
                // 已经收到操作消息的事务半消息偏移量，需要被移除。key：半消息偏移量，value：操作消息偏移量
                HashMap<Long, Long> removeMap = new HashMap<>();
                // 在操作消息中的所有半消息的偏移量
                HashMap<Long, HashSet<Long>> opMsgMap = new HashMap<Long, HashSet<Long>>();
                // 拉取一批（32 条）操作队列的消息，填充 removeMap 和 opMsgMap。对于这批操作消息已经提交的事务半消息，不用回查需要移除
                PullResult pullResult = fillOpRemoveMap(removeMap, opQueue, opOffset, halfOffset, opMsgMap, doneOpOffset);
                if (null == pullResult) {
                    log.error("The queue={} check msgOffset={} with opOffset={} failed, pullResult is null",
                        messageQueue, halfOffset, opOffset);
                    continue;
                }
                // single thread
                // 获取空消息的次数
                int getMessageNullCount = 1;
                // 事务半消息队列的最新消费进度
                long newOffset = halfOffset;
                // 当前处理回查的事务半消息消费偏移量，从当前消费进度开始遍历
                long i = halfOffset;
                long nextOpOffset = pullResult.getNextBeginOffset();
                // 重新放入事务半消息队列的事务半消息数量
                int putInQueueCount = 0;
                int escapeFailCnt = 0;

                // 不断循环消费事务半消息进行处理，直到没有新的半消息或处理持续时间超过 60s
                while (true) {
                    // 对该队列的消息的回查最多持续 60s
                    if (System.currentTimeMillis() - startTime > MAX_PROCESS_TIME_LIMIT) {
                        log.info("Queue={} process time reach max={}", messageQueue, MAX_PROCESS_TIME_LIMIT);
                        break;
                    }
                    // 如果 removeMap 中包含该事务半消息的偏移量，说明该事务半消息已经被提交或者回滚，需要移除，不需要回查
                    if (removeMap.containsKey(i)) {
                        log.debug("Half offset {} has been committed/rolled back", i);
                        Long removedOpOffset = removeMap.remove(i);
                        opMsgMap.get(removedOpOffset).remove(i);
                        if (opMsgMap.get(removedOpOffset).size() == 0) {
                            opMsgMap.remove(removedOpOffset);
                            doneOpOffset.add(removedOpOffset);
                        }
                    // 该事务半消息没有被提交或者回滚，需要回查。查询出该事务半消息的消息体
                    } else {
                        GetResult getResult = getHalfMsg(messageQueue, i);
                        // 事务半消息
                        MessageExt msgExt = getResult.getMsg();
                        if (msgExt == null) {
                            // 如果事务半消息 Topic 中获取不到消息，且超过最大获取不到消息的次数（默认 1 次），则结束本次回查
                            if (getMessageNullCount++ > MAX_RETRY_COUNT_WHEN_HALF_NULL) {
                                break;
                            }
                            if (getResult.getPullResult().getPullStatus() == PullStatus.NO_NEW_MSG) {
                                log.debug("No new msg, the miss offset={} in={}, continue check={}, pull result={}", i,
                                    messageQueue, getMessageNullCount, getResult.getPullResult());
                                break;
                            } else {
                                // 传入的偏移量非法，修正偏移量后继续查询事务半消息
                                log.info("Illegal offset, the miss offset={} in={}, continue check={}, pull result={}",
                                    i, messageQueue, getMessageNullCount, getResult.getPullResult());
                                i = getResult.getPullResult().getNextBeginOffset();
                                newOffset = i;
                                continue;
                            }
                        }

                        // 事务消息逃逸
                        if (this.transactionalMessageBridge.getBrokerController().getBrokerConfig().isEnableSlaveActingMaster()
                            && this.transactionalMessageBridge.getBrokerController().getMinBrokerIdInGroup()
                            == this.transactionalMessageBridge.getBrokerController().getBrokerIdentity().getBrokerId()
                            && BrokerRole.SLAVE.equals(this.transactionalMessageBridge.getBrokerController().getMessageStoreConfig().getBrokerRole())
                        ) {
                            final MessageExtBrokerInner msgInner = this.transactionalMessageBridge.renewHalfMessageInner(msgExt);
                            final boolean isSuccess = this.transactionalMessageBridge.escapeMessage(msgInner);

                            if (isSuccess) {
                                escapeFailCnt = 0;
                                newOffset = i + 1;
                                i++;
                            } else {
                                log.warn("Escaping transactional message failed {} times! msgId(offsetId)={}, UNIQ_KEY(transactionId)={}",
                                    escapeFailCnt + 1,
                                    msgExt.getMsgId(),
                                    msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
                                if (escapeFailCnt < MAX_RETRY_TIMES_FOR_ESCAPE) {
                                    escapeFailCnt++;
                                    Thread.sleep(100L * (2 ^ escapeFailCnt));
                                } else {
                                    escapeFailCnt = 0;
                                    newOffset = i + 1;
                                    i++;
                                }
                            }
                            continue;
                        }

                        // 判断事务消息是否需要丢弃，即超过最大回查次数（15次）；或者是否需要跳过，即超过文件保存时间。
                        // 如果不丢弃或跳过，这里会增加事务半消息属性中的重试次数
                        if (needDiscard(msgExt, transactionCheckMax) || needSkip(msgExt)) {
                            // 丢弃或跳过：将事务半消息发送到特殊的内部 Topic：TRANS_CHECK_MAX
                            listener.resolveDiscardMsg(msgExt);
                            newOffset = i + 1;
                            i++;
                            continue;
                        }
                        // 如果事务半消息的存储时间大于等于本次回查开始时间，说明这条是新的事务半消息存储进来，结束本次回查，稍后再回查
                        if (msgExt.getStoreTimestamp() >= startTime) {
                            log.debug("Fresh stored. the miss offset={}, check it later, store={}", i,
                                new Date(msgExt.getStoreTimestamp()));
                            break;
                        }

                        // 计算事务半消息是否处在免疫回查期：即事务消息发送一段时间之内不进行回查
                        // 事务半消息已经存在的时长
                        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();
                        // 免疫回查期时长，默认等于事务超时时间，6s
                        long checkImmunityTime = transactionTimeout;
                        // 消息属性中定义的免疫回查时长
                        String checkImmunityTimeStr = msgExt.getUserProperty(MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS);
                        if (null != checkImmunityTimeStr) {
                            // 如果消息属性中也定义了免疫回查时长，优先使用消息属性中的值，单位为秒
                            checkImmunityTime = getImmunityTime(checkImmunityTimeStr, transactionTimeout);
                            // 事务半消息存在时长小于免疫回查时长，不需要回查
                            if (valueOfCurrentMinusBorn < checkImmunityTime) {
                                // 检查该事务半消息的偏移量，如果在 removeMap 里，即该消息已经被提交或回滚，不需要处理了。
                                // 跳过，继续下一个事务半消息的回查判断
                                if (checkPrepareQueueOffset(removeMap, doneOpOffset, msgExt, checkImmunityTimeStr)) {
                                    newOffset = i + 1;
                                    i++;
                                    continue;
                                }
                            }
                        } else {
                            if (0 <= valueOfCurrentMinusBorn && valueOfCurrentMinusBorn < checkImmunityTime) {
                                log.debug("New arrived, the miss offset={}, check it later checkImmunity={}, born={}", i,
                                    checkImmunityTime, new Date(msgExt.getBornTimestamp()));
                                break;
                            }
                        }
                        /*
                         * 判断是否需要回查，满足以下 3 个条件之一则进行回查
                         * 1. 没有操作消息，且当前半消息在回查免疫期外
                         * 2. 存在操作消息，且本批次操作消息中最后一个在免疫期外
                         * 3. Broker 与客户端有时间差
                         */
                        List<MessageExt> opMsg = pullResult == null ? null : pullResult.getMsgFoundList();
                        boolean isNeedCheck = opMsg == null && valueOfCurrentMinusBorn > checkImmunityTime
                            || opMsg != null && opMsg.get(opMsg.size() - 1).getBornTimestamp() - startTime > transactionTimeout
                            || valueOfCurrentMinusBorn <= -1;

                        if (isNeedCheck) {
                            // 将事务半消息重新放入到事务半消息队列中，因为前面更新了事务半消息的属性（回查次数），需要更新消费偏移量并且重新存储。
                            // 并且回查是一个异步过程，不确定回查是否能够请求到，所以这里做最坏的打算，没有请求成功则下次继续回查。
                            // 如果回查成功则写入操作消息 Map，下次不会回查。
                            // 这里有个问题是：最坏的情况下（事务消息一直执行中，不停回查），会最多重复存储 15 次事务半消息，造成写放大。
                            if (!putBackHalfMsgQueue(msgExt, i)) {
                                continue;
                            }
                            putInQueueCount++;
                            log.info("Check transaction. real_topic={},uniqKey={},offset={},commitLogOffset={}",
                                    msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC),
                                    msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                                    msgExt.getQueueOffset(), msgExt.getCommitLogOffset());
                            // 通过 listener 向生产者客户端发送单向的消息回查请求
                            listener.resolveHalfMsg(msgExt);
                        } else {
                            // 不需要进行回查，更新下一个要检查的事务操作消息的偏移量
                            nextOpOffset = pullResult != null ? pullResult.getNextBeginOffset() : nextOpOffset;
                            pullResult = fillOpRemoveMap(removeMap, opQueue, nextOpOffset,
                                    halfOffset, opMsgMap, doneOpOffset);
                            if (pullResult == null || pullResult.getPullStatus() == PullStatus.NO_NEW_MSG
                                    || pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL
                                    || pullResult.getPullStatus() == PullStatus.NO_MATCHED_MSG) {

                                try {
                                    Thread.sleep(SLEEP_WHILE_NO_OP);
                                } catch (Throwable ignored) {
                                }

                            } else {
                                log.info("The miss message offset:{}, pullOffsetOfOp:{}, miniOffset:{} get more opMsg.", i, nextOpOffset, halfOffset);
                            }

                            continue;
                        }
                    }
                    newOffset = i + 1;
                    i++;
                }
                // 更新事务半消息消费队列的回查进度
                if (newOffset != halfOffset) {
                    transactionalMessageBridge.updateConsumeOffset(messageQueue, newOffset);
                }
                // 更新操作队列的消费进度
                long newOpOffset = calculateOpOffset(doneOpOffset, opOffset);
                if (newOpOffset != opOffset) {
                    transactionalMessageBridge.updateConsumeOffset(opQueue, newOpOffset);
                }
                GetResult getResult = getHalfMsg(messageQueue, newOffset);
                pullResult = pullOpMsg(opQueue, newOpOffset, 1);
                long maxMsgOffset = getResult.getPullResult() == null ? newOffset : getResult.getPullResult().getMaxOffset();
                long maxOpOffset = pullResult == null ? newOpOffset : pullResult.getMaxOffset();
                long msgTime = getResult.getMsg() == null ? System.currentTimeMillis() : getResult.getMsg().getStoreTimestamp();

                log.info("After check, {} opOffset={} opOffsetDiff={} msgOffset={} msgOffsetDiff={} msgTime={} msgTimeDelayInMs={} putInQueueCount={}",
                        messageQueue, newOpOffset, maxOpOffset - newOpOffset, newOffset, maxMsgOffset - newOffset, new Date(msgTime),
                        System.currentTimeMillis() - msgTime, putInQueueCount);
            }
        } catch (Throwable e) {
            log.error("Check error", e);
        }

    }

    /**
     * 计算免疫回查时长，如果消息属性中定义了免疫回查时长，则使用消息属性中定义的免疫回查时长，否则使用事务超时时间
     * @param checkImmunityTimeStr 消息属性中定义的免疫回查时长
     * @param transactionTimeout 事务超时时间
     * @return 免疫回查时长
     */
    private long getImmunityTime(String checkImmunityTimeStr, long transactionTimeout) {
        long checkImmunityTime;

        checkImmunityTime = getLong(checkImmunityTimeStr);
        if (-1 == checkImmunityTime) {
            checkImmunityTime = transactionTimeout;
        } else {
            checkImmunityTime *= 1000;
        }
        return checkImmunityTime;
    }

    /**
     * Read op message, parse op message, and fill removeMap
     * 拉取一批事务操作消息，解析它们对应的事务半消息，生成需要移除的事务半消息 Map，即 removeMap
     *
     * @param removeMap Half message to be remove, key:halfOffset, value: opOffset.
     *                  需要删除的事务半消息，Key：半消息偏移量，Value：操作消息偏移量
     * @param opQueue Op message queue.
     *                事务操作消息队列
     * @param pullOffsetOfOp The begin offset of op message queue.
     *                       事务消息操作队列的最新消费进度
     * @param miniOffset The current minimum offset of half message queue.
     *                   事务半消息队列的最新消费进度
     * @param opMsgMap Half message offset in op message
     *                 在操作消息中的所有半消息的偏移量
     * @param doneOpOffset Stored op messages that have been processed.
     *                     已经处理完的操作消息的偏移量列表
     * @return Op message result.
     */
    private PullResult fillOpRemoveMap(HashMap<Long, Long> removeMap, MessageQueue opQueue,
                                       long pullOffsetOfOp, long miniOffset, Map<Long, HashSet<Long>> opMsgMap, List<Long> doneOpOffset) {
        // 用 CID_SYS_RMQ_TRANS 消费组拉取 32 条事务操作消息
        // 这里有个问题，没有拉取全部的事务操作消息，有可能后面的操作消息将前面的操作消息的事务半消息提交或回滚了，但是这里没有拉取到
        // 导致事务半消息不会被移除，需要进行多余的回查。这里可能是为了减少拉取消息和处理的时间，所以在拉取数量上做了权衡。
        PullResult pullResult = pullOpMsg(opQueue, pullOffsetOfOp, OP_MSG_PULL_NUMS);
        if (null == pullResult) {
            return null;
        }
        if (pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL
            || pullResult.getPullStatus() == PullStatus.NO_MATCHED_MSG) {
            log.warn("The miss op offset={} in queue={} is illegal, pullResult={}", pullOffsetOfOp, opQueue,
                pullResult);
            transactionalMessageBridge.updateConsumeOffset(opQueue, pullResult.getNextBeginOffset());
            return pullResult;
        } else if (pullResult.getPullStatus() == PullStatus.NO_NEW_MSG) {
            log.warn("The miss op offset={} in queue={} is NO_NEW_MSG, pullResult={}", pullOffsetOfOp, opQueue,
                pullResult);
            return pullResult;
        }
        List<MessageExt> opMsg = pullResult.getMsgFoundList();
        if (opMsg == null) {
            log.warn("The miss op offset={} in queue={} is empty, pullResult={}", pullOffsetOfOp, opQueue, pullResult);
            return pullResult;
        }
        // 遍历拉取到的事务操作消息
        for (MessageExt opMessageExt : opMsg) {
            if (opMessageExt.getBody() == null) {
                log.error("op message body is null. queueId={}, offset={}", opMessageExt.getQueueId(),
                        opMessageExt.getQueueOffset());
                doneOpOffset.add(opMessageExt.getQueueOffset());
                continue;
            }
            HashSet<Long> set = new HashSet<Long>();
            String queueOffsetBody = new String(opMessageExt.getBody(), TransactionalMessageUtil.CHARSET);

            log.debug("Topic: {} tags: {}, OpOffset: {}, HalfOffset: {}", opMessageExt.getTopic(),
                    opMessageExt.getTags(), opMessageExt.getQueueOffset(), queueOffsetBody);
            // 如果操作消息的 TAG 为 REMOVE_TAG，则表示它对应的事务半消息已经被提交或者回滚，需要移除。
            if (TransactionalMessageUtil.REMOVE_TAG.equals(opMessageExt.getTags())) {
                // 解析出操作消息中的多个事务半消息的偏移量
                String[] offsetArray = queueOffsetBody.split(TransactionalMessageUtil.OFFSET_SEPARATOR);
                for (String offset : offsetArray) {
                    Long offsetValue = getLong(offset);
                    // 如果事务半消息的偏移量小于当前已经消费的事务半消息的偏移量，说明已经处理过，跳过
                    if (offsetValue < miniOffset) {
                        continue;
                    }

                    // 把未消费过的事务半消息的偏移量放入 removeMap 中，表示该事务半消息需要移除，不需要回查
                    removeMap.put(offsetValue, opMessageExt.getQueueOffset());
                    set.add(offsetValue);
                }
            } else {
                log.error("Found a illegal tag in opMessageExt= {} ", opMessageExt);
            }

            if (set.size() > 0) {
                // 加入操作消息对应的事务半消息集合中
                opMsgMap.put(opMessageExt.getQueueOffset(), set);
            } else {
                doneOpOffset.add(opMessageExt.getQueueOffset());
            }
        }

        log.debug("Remove map: {}", removeMap);
        log.debug("Done op list: {}", doneOpOffset);
        log.debug("opMsg map: {}", opMsgMap);
        return pullResult;
    }

    /**
     * If return true, skip this msg
     *
     * @param removeMap Op message map to determine whether a half message was responded by producer.
     * @param doneOpOffset Op Message which has been checked.
     * @param msgExt Half message
     * @return Return true if put success, otherwise return false.
     */
    private boolean checkPrepareQueueOffset(HashMap<Long, Long> removeMap, List<Long> doneOpOffset,
        MessageExt msgExt, String checkImmunityTimeStr) {
        String prepareQueueOffsetStr = msgExt.getUserProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET);
        if (null == prepareQueueOffsetStr) {
            return putImmunityMsgBackToHalfQueue(msgExt);
        } else {
            long prepareQueueOffset = getLong(prepareQueueOffsetStr);
            if (-1 == prepareQueueOffset) {
                return false;
            } else {
                if (removeMap.containsKey(prepareQueueOffset)) {
                    long tmpOpOffset = removeMap.remove(prepareQueueOffset);
                    doneOpOffset.add(tmpOpOffset);
                    log.info("removeMap contain prepareQueueOffset. real_topic={},uniqKey={},immunityTime={},offset={}",
                            msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC),
                            msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                            checkImmunityTimeStr,
                            msgExt.getQueueOffset());
                    return true;
                } else {
                    return putImmunityMsgBackToHalfQueue(msgExt);
                }
            }
        }
    }

    /**
     * Write messageExt to Half topic again
     *
     * @param messageExt Message will be write back to queue
     * @return Put result can used to determine the specific results of storage.
     */
    private PutMessageResult putBackToHalfQueueReturnResult(MessageExt messageExt) {
        PutMessageResult putMessageResult = null;
        try {
            MessageExtBrokerInner msgInner = transactionalMessageBridge.renewHalfMessageInner(messageExt);
            putMessageResult = transactionalMessageBridge.putMessageReturnResult(msgInner);
        } catch (Exception e) {
            log.warn("PutBackToHalfQueueReturnResult error", e);
        }
        return putMessageResult;
    }

    private boolean putImmunityMsgBackToHalfQueue(MessageExt messageExt) {
        MessageExtBrokerInner msgInner = transactionalMessageBridge.renewImmunityHalfMessageInner(messageExt);
        return transactionalMessageBridge.putMessage(msgInner);
    }

    /**
     * Read half message from Half Topic
     *
     * @param mq Target message queue, in this method, it means the half message queue.
     * @param offset Offset in the message queue.
     * @param nums Pull message number.
     * @return Messages pulled from half message queue.
     */
    private PullResult pullHalfMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getHalfMessage(mq.getQueueId(), offset, nums);
    }

    /**
     * Read op message from Op Topic
     *
     * @param mq Target Message Queue
     * @param offset Offset in the message queue
     * @param nums Pull message number
     * @return Messages pulled from operate message queue.
     */
    private PullResult pullOpMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getOpMessage(mq.getQueueId(), offset, nums);
    }

    private Long getLong(String s) {
        long v = -1;
        try {
            v = Long.parseLong(s);
        } catch (Exception e) {
            log.error("GetLong error", e);
        }
        return v;

    }

    private Integer getInt(String s) {
        int v = -1;
        try {
            v = Integer.parseInt(s);
        } catch (Exception e) {
            log.error("GetInt error", e);
        }
        return v;

    }

    private long calculateOpOffset(List<Long> doneOffset, long oldOffset) {
        Collections.sort(doneOffset);
        long newOffset = oldOffset;
        for (int i = 0; i < doneOffset.size(); i++) {
            if (doneOffset.get(i) == newOffset) {
                newOffset++;
            } else {
                break;
            }
        }
        return newOffset;

    }

    /**
     * 获取事务半消息对应的操作队列，如果 {@link TransactionalMessageServiceImpl#opQueueMap} 没有存，则创建一个并放入。
     * 该主题保存事务消息提交或者回滚的请求
     *
     * @param messageQueue 事务半消息队列
     * @return 操作队列，主题为 {@link TopicValidator#RMQ_SYS_TRANS_OP_HALF_TOPIC}
     */
    private MessageQueue getOpQueue(MessageQueue messageQueue) {
        MessageQueue opQueue = opQueueMap.get(messageQueue);
        if (opQueue == null) {
            opQueue = new MessageQueue(TransactionalMessageUtil.buildOpTopic(), messageQueue.getBrokerName(),
                messageQueue.getQueueId());
            opQueueMap.put(messageQueue, opQueue);
        }
        return opQueue;

    }

    private GetResult getHalfMsg(MessageQueue messageQueue, long offset) {
        GetResult getResult = new GetResult();

        PullResult result = pullHalfMsg(messageQueue, offset, PULL_MSG_RETRY_NUMBER);
        if (result != null) {
            getResult.setPullResult(result);
            List<MessageExt> messageExts = result.getMsgFoundList();
            if (messageExts == null || messageExts.size() == 0) {
                return getResult;
            }
            getResult.setMsg(messageExts.get(0));
        }
        return getResult;
    }

    private OperationResult getHalfMessageByOffset(long commitLogOffset) {
        OperationResult response = new OperationResult();
        MessageExt messageExt = this.transactionalMessageBridge.lookMessageByOffset(commitLogOffset);
        if (messageExt != null) {
            response.setPrepareMessage(messageExt);
            response.setResponseCode(ResponseCode.SUCCESS);
        } else {
            response.setResponseCode(ResponseCode.SYSTEM_ERROR);
            response.setResponseRemark("Find prepared transaction message failed");
        }
        return response;
    }

    /**
     * 删除事务半消息
     *
     * @param messageExt 事务半消息
     * @return
     */
    @Override
    public boolean deletePrepareMessage(MessageExt messageExt) {
        Integer queueId = messageExt.getQueueId();
        // 创建事务操作消息批量提交的上下文（如果不存在）
        MessageQueueOpContext mqContext = deleteContext.get(queueId);
        if (mqContext == null) {
            mqContext = new MessageQueueOpContext(System.currentTimeMillis(), 20000);
            MessageQueueOpContext old = deleteContext.putIfAbsent(queueId, mqContext);
            if (old != null) {
                mqContext = old;
            }
        }

        // 将事务半消息逻辑偏移量加入到操作上下文中，让一条事务操作消息能对应多条事务半消息，同时也作为内存缓存
        String data = messageExt.getQueueOffset() + TransactionalMessageUtil.OFFSET_SEPARATOR;
        try {
            boolean res = mqContext.getContextQueue().offer(data, 100, TimeUnit.MILLISECONDS);
            if (res) {
                // 如果操作上下文中的消息总大小超过了 Broker 配置的最大事务操作消息大小（默认 4096），则唤醒事务操作消息批量提交服务，
                // 将之前的事务半消息生成一条事务操作消息写盘，然后直接返回
                int totalSize = mqContext.getTotalSize().addAndGet(data.length());
                if (totalSize > transactionalMessageBridge.getBrokerController().getBrokerConfig().getTransactionOpMsgMaxSize()) {
                    this.transactionalOpBatchService.wakeup();
                }
                return true;
            } else {
                // 如果操作上下文队列已满，则唤醒事务操作消息批量提交服务，将之前的事务半消息生成一条事务操作消息写盘。
                this.transactionalOpBatchService.wakeup();
            }
        } catch (InterruptedException ignore) {
        }

        // 上下文操作队列已满的情况，这条事务操作消息没有加入事务操作消息批量提交的上下文，所以单独写这条事务操作消息。
        Message msg = getOpMessage(queueId, data);
        if (this.transactionalMessageBridge.writeOp(queueId, msg)) {
            log.warn("Force add remove op data. queueId={}", queueId);
            return true;
        } else {
            log.error("Transaction op message write failed. messageId is {}, queueId is {}", messageExt.getMsgId(), messageExt.getQueueId());
            return false;
        }
    }

    @Override
    public OperationResult commitMessage(EndTransactionRequestHeader requestHeader) {
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    @Override
    public OperationResult rollbackMessage(EndTransactionRequestHeader requestHeader) {
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    @Override
    public boolean open() {
        return true;
    }

    @Override
    public void close() {
        if (this.transactionalOpBatchService != null) {
            this.transactionalOpBatchService.shutdown();
        }
        this.getTransactionMetrics().persist();
    }

    /**
     * 生成事务操作消息
     *
     * @param queueId 事务半消息队列 ID
     * @param moreData 事务半消息逻辑偏移量
     * @return
     */
    public Message getOpMessage(int queueId, String moreData) {
        String opTopic = TransactionalMessageUtil.buildOpTopic();
        MessageQueueOpContext mqContext = deleteContext.get(queueId);

        int moreDataLength = moreData != null ? moreData.length() : 0;
        int length = moreDataLength;
        int maxSize = transactionalMessageBridge.getBrokerController().getBrokerConfig().getTransactionOpMsgMaxSize();
        if (length < maxSize) {
            int sz = mqContext.getTotalSize().get();
            if (sz > maxSize || length + sz > maxSize) {
                length = maxSize + 100;
            } else {
                length += sz;
            }
        }

        StringBuilder sb = new StringBuilder(length);

        if (moreData != null) {
            sb.append(moreData);
        }

        while (!mqContext.getContextQueue().isEmpty()) {
            if (sb.length() >= maxSize) {
                break;
            }
            String data = mqContext.getContextQueue().poll();
            if (data != null) {
                sb.append(data);
            }
        }

        if (sb.length() == 0) {
            return null;
        }

        int l = sb.length() - moreDataLength;
        mqContext.getTotalSize().addAndGet(-l);
        mqContext.setLastWriteTimestamp(System.currentTimeMillis());
        return new Message(opTopic, TransactionalMessageUtil.REMOVE_TAG,
                sb.toString().getBytes(TransactionalMessageUtil.CHARSET));
    }
    public long batchSendOpMessage() {
        long startTime = System.currentTimeMillis();
        try {
            long firstTimestamp = startTime;
            Map<Integer, Message> sendMap = null;
            long interval = transactionalMessageBridge.getBrokerController().getBrokerConfig().getTransactionOpBatchInterval();
            int maxSize = transactionalMessageBridge.getBrokerController().getBrokerConfig().getTransactionOpMsgMaxSize();
            boolean overSize = false;
            for (Map.Entry<Integer, MessageQueueOpContext> entry : deleteContext.entrySet()) {
                MessageQueueOpContext mqContext = entry.getValue();
                //no msg in contextQueue
                if (mqContext.getTotalSize().get() <= 0 || mqContext.getContextQueue().size() == 0 ||
                        // wait for the interval
                        mqContext.getTotalSize().get() < maxSize &&
                                startTime - mqContext.getLastWriteTimestamp() < interval) {
                    continue;
                }

                if (sendMap == null) {
                    sendMap = new HashMap<>();
                }

                Message opMsg = getOpMessage(entry.getKey(), null);
                if (opMsg == null) {
                    continue;
                }
                sendMap.put(entry.getKey(), opMsg);
                firstTimestamp = Math.min(firstTimestamp, mqContext.getLastWriteTimestamp());
                if (mqContext.getTotalSize().get() >= maxSize) {
                    overSize = true;
                }
            }

            if (sendMap != null) {
                for (Map.Entry<Integer, Message> entry : sendMap.entrySet()) {
                    if (!this.transactionalMessageBridge.writeOp(entry.getKey(), entry.getValue())) {
                        log.error("Transaction batch op message write failed. body is {}, queueId is {}",
                                new String(entry.getValue().getBody(), TransactionalMessageUtil.CHARSET), entry.getKey());
                    }
                }
            }

            log.debug("Send op message queueIds={}", sendMap == null ? null : sendMap.keySet());

            //wait for next batch remove
            long wakeupTimestamp = firstTimestamp + interval;
            if (!overSize && wakeupTimestamp > startTime) {
                return wakeupTimestamp;
            }
        } catch (Throwable t) {
            log.error("batchSendOp error.", t);
        }

        return 0L;
    }

    public Map<Integer, MessageQueueOpContext> getDeleteContext() {
        return this.deleteContext;
    }
}
