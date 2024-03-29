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
package org.apache.rocketmq.broker.longpolling;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.ConsumeQueueExt;

/**
 * 长轮询请求管理容器
 * <p>
 * 如果拉消息为空，在这里hold住；每隔一秒钟检测一下是否有数据到来，有数据就触发相应的请求，然后发送响应
 * <p>
 * 长轮询请求如果到了超时时间，则不继续维护，直接触发请求
 */
public class PullRequestHoldService extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    protected static final String TOPIC_QUEUEID_SEPARATOR = "@";
    protected final BrokerController brokerController;
    private final SystemClock systemClock = new SystemClock();
    // 消息拉取请求容器
    protected ConcurrentMap<String/* topic@queueId */, ManyPullRequest> pullRequestTable =
        new ConcurrentHashMap<>(1024);

    public PullRequestHoldService(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * 挂起（保存）客户端请求，当有数据的时候触发请求
     *
     * @param topic 主题
     * @param queueId 队列编号
     * @param pullRequest 拉取消息请求
     */
    public void suspendPullRequest(final String topic, final int queueId, final PullRequest pullRequest) {
        // 根据topic和queueId构造map的key
        String key = this.buildKey(topic, queueId);
        // map的key如果为空，创建一个空的request队列，填充key和value
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (null == mpr) {
            mpr = new ManyPullRequest();
            ManyPullRequest prev = this.pullRequestTable.putIfAbsent(key, mpr);
            if (prev != null) {
                mpr = prev;
            }
        }

        pullRequest.getRequestCommand().setSuspended(true);
        // 保存该次Consumer拉取请求
        mpr.addPullRequest(pullRequest);
    }

    private String buildKey(final String topic, final int queueId) {
        StringBuilder sb = new StringBuilder(topic.length() + 5);
        sb.append(topic);
        sb.append(TOPIC_QUEUEID_SEPARATOR);
        sb.append(queueId);
        return sb.toString();
    }

    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        while (!this.isStopped()) {
            try {
                // 等待一定时间
                if (this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                    // 开启长轮询，每5s判断一次消息是否到达
                    this.waitForRunning(5 * 1000);
                } else {
                    // 未开启长轮询，每1s判断一次消息是否到达
                    this.waitForRunning(this.brokerController.getBrokerConfig().getShortPollingTimeMills());
                }

                long beginLockTimestamp = this.systemClock.now();
                // 检查是否有消息到达，可以唤醒挂起的请求
                this.checkHoldRequest();
                long costTime = this.systemClock.now() - beginLockTimestamp;
                if (costTime > 5 * 1000) {
                    log.warn("PullRequestHoldService: check hold pull request cost {}ms", costTime);
                }
            } catch (Throwable e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info("{} service end", this.getServiceName());
    }

    @Override
    public String getServiceName() {
        if (brokerController != null && brokerController.getBrokerConfig().isInBrokerContainer()) {
            return this.brokerController.getBrokerIdentity().getIdentifier() + PullRequestHoldService.class.getSimpleName();
        }
        return PullRequestHoldService.class.getSimpleName();
    }

    /**
     * 检查所有已经挂起的长轮询请求
     * 如果有数据满足要求，就触发请求再次执行
     */
    protected void checkHoldRequest() {
        // 遍历拉取请求容器中的每个队列
        for (String key : this.pullRequestTable.keySet()) {
            String[] kArray = key.split(TOPIC_QUEUEID_SEPARATOR);
            if (2 == kArray.length) {
                String topic = kArray[0];
                int queueId = Integer.parseInt(kArray[1]);
                // 从store中获取队列的最大偏移量
                final long offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                try {
                    // 根据store中获取的最大偏移量，判断是否有新消息到达，如果有则执行拉取请求操作
                    this.notifyMessageArriving(topic, queueId, offset);
                } catch (Throwable e) {
                    log.error(
                        "PullRequestHoldService: failed to check hold request failed, topic={}, queueId={}", topic,
                        queueId, e);
                }
            }
        }
    }

    /**
     * 当有新消息到达的时候，唤醒长轮询的消费端请求
     *
     * @param topic     消息Topic
     * @param queueId   消息队列ID
     * @param maxOffset 消费队列的最大Offset
     */
    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset) {
        notifyMessageArriving(topic, queueId, maxOffset, null, 0, null, null);
    }

    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset, final Long tagsCode,
        long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        // 根据topic和queueId从容器中取出挂起的拉取请求列表
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (mpr != null) {
            // 获取挂起的拉取请求列表
            List<PullRequest> requestList = mpr.cloneListAndClear();
            if (requestList != null) {
                // 预先定义需要继续挂起的拉取请求列表
                List<PullRequest> replayList = new ArrayList<>();

                for (PullRequest request : requestList) {
                    long newestOffset = maxOffset;
                    // 从store中获取该队列消息的最大offset
                    if (newestOffset <= request.getPullFromThisOffset()) {
                        newestOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                    }

                    // 消费队列最大offset比消费者拉取请求的offset大，说明有新的消息可以被拉取
                    if (newestOffset > request.getPullFromThisOffset()) {
                        // 消息过滤匹配
                        boolean match = request.getMessageFilter().isMatchedByConsumeQueue(tagsCode,
                            new ConsumeQueueExt.CqExtUnit(tagsCode, msgStoreTime, filterBitMap));
                        // match by bit map, need eval again when properties is not null.
                        if (match && properties != null) {
                            match = request.getMessageFilter().isMatchedByCommitLog(null, properties);
                        }

                        if (match) {
                            try {
                                // 会调用PullMessageProcessor#processRequest方法拉取消息，然后将结果返回给消费者
                                this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                    request.getRequestCommand());
                            } catch (Throwable e) {
                                log.error(
                                    "PullRequestHoldService#notifyMessageArriving: failed to execute request when "
                                        + "message matched, topic={}, queueId={}", topic, queueId, e);
                            }
                            continue;
                        }
                    }

                    // 查看是否超时，如果Consumer请求达到了超时时间，也触发响应，直接返回消息未找到
                    if (System.currentTimeMillis() >= (request.getSuspendTimestamp() + request.getTimeoutMillis())) {
                        try {
                            this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                request.getRequestCommand());
                        } catch (Throwable e) {
                            log.error(
                                "PullRequestHoldService#notifyMessageArriving: failed to execute request when time's "
                                    + "up, topic={}, queueId={}", topic, queueId, e);
                        }
                        continue;
                    }
                    // 当前不满足要求，重新放回Hold列表中
                    replayList.add(request);
                }

                if (!replayList.isEmpty()) {
                    mpr.addPullRequest(replayList);
                }
            }
        }
    }

    public void notifyMasterOnline() {
        for (ManyPullRequest mpr : this.pullRequestTable.values()) {
            if (mpr == null || mpr.isEmpty()) {
                continue;
            }
            for (PullRequest request : mpr.cloneListAndClear()) {
                try {
                    log.info("notify master online, wakeup {} {}", request.getClientChannel(), request.getRequestCommand());
                    this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                        request.getRequestCommand());
                } catch (Throwable e) {
                    log.error("execute request when master online failed.", e);
                }
            }
        }

    }
}
