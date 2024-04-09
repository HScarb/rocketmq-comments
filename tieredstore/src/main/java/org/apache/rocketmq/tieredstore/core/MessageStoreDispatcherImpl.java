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
package org.apache.rocketmq.tieredstore.core;

import io.opentelemetry.api.common.Attributes;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.MessageStoreExecutor;
import org.apache.rocketmq.tieredstore.TieredMessageStore;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.file.FlatFileInterface;
import org.apache.rocketmq.tieredstore.file.FlatFileStore;
import org.apache.rocketmq.tieredstore.file.FlatMessageFile;
import org.apache.rocketmq.tieredstore.index.IndexService;
import org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant;
import org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsManager;
import org.apache.rocketmq.tieredstore.util.MessageFormatUtil;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 分级存储消息分发器，每当有消息发送到 Broker ，保存到 {@link org.apache.rocketmq.store.CommitLog} 后，
 * 会调用 {@link #dispatch(DispatchRequest)} 方法
 */
public class MessageStoreDispatcherImpl extends ServiceThread implements MessageStoreDispatcher {

    protected static final Logger log = LoggerFactory.getLogger(MessageStoreUtil.TIERED_STORE_LOGGER_NAME);

    protected final String brokerName;
    protected final MessageStore defaultStore;
    protected final MessageStoreConfig storeConfig;
    protected final TieredMessageStore messageStore;
    protected final FlatFileStore flatFileStore;
    protected final MessageStoreExecutor storeExecutor;
    protected final MessageStoreFilter topicFilter;
    protected final Semaphore semaphore;
    protected final IndexService indexService;

    public MessageStoreDispatcherImpl(TieredMessageStore messageStore) {
        this.messageStore = messageStore;
        this.storeConfig = messageStore.getStoreConfig();
        this.defaultStore = messageStore.getDefaultStore();
        this.brokerName = storeConfig.getBrokerName();
        this.semaphore = new Semaphore(
            this.storeConfig.getTieredStoreMaxPendingLimit() / 4);
        this.topicFilter = messageStore.getTopicFilter();
        this.flatFileStore = messageStore.getFlatFileStore();
        this.storeExecutor = messageStore.getStoreExecutor();
        this.indexService = messageStore.getIndexService();
    }

    @Override
    public String getServiceName() {
        return MessageStoreDispatcher.class.getSimpleName();
    }

    public void dispatchWithSemaphore(FlatFileInterface flatFile) {
        try {
            if (stopped) {
                return;
            }
            semaphore.acquire();
            this.doScheduleDispatch(flatFile, false)
                .whenComplete((future, throwable) -> semaphore.release());
        } catch (InterruptedException e) {
            semaphore.release();
        }
    }

    /**
     * 分级存储消息分发入口，根据分发的请求创建新的 {@link FlatMessageFile} 文件
     *
     * @param request dispatch message request
     */
    @Override
    public void dispatch(DispatchRequest request) {
        if (stopped || topicFilter != null && topicFilter.filterTopic(request.getTopic())) {
            return;
        }
        flatFileStore.computeIfAbsent(
            new MessageQueue(request.getTopic(), brokerName, request.getQueueId()));
    }

    /**
     * 分发消息，将消息写入到 {@link FlatMessageFile} 文件中
     *
     * @param flatFile
     * @param force true: 等待直到获取锁成功，false: 获取锁失败时直接返回
     * @return
     */
    @Override
    public CompletableFuture<Boolean> doScheduleDispatch(FlatFileInterface flatFile, boolean force) {
        if (stopped) {
            return CompletableFuture.completedFuture(true);
        }

        String topic = flatFile.getMessageQueue().getTopic();
        int queueId = flatFile.getMessageQueue().getQueueId();

        // 获取分级存储文件锁，写消息。force 为 true 时，会待直到获取锁成功
        // For test scenarios, we set the 'force' variable to true to
        // ensure that the data in the cache is directly committed successfully.
        force = !storeConfig.isTieredStoreGroupCommit() || force;
        if (force) {
            flatFile.getFileLock().lock();
        } else {
            if (!flatFile.getFileLock().tryLock()) {
                return CompletableFuture.completedFuture(false);
            }
        }

        try {
            // 如果 Topic 被过滤，则直接销毁文件
            if (topicFilter != null && topicFilter.filterTopic(flatFile.getMessageQueue().getTopic())) {
                flatFileStore.destroyFile(flatFile.getMessageQueue());
                return CompletableFuture.completedFuture(false);
            }

            // 已经提交到缓冲区的 ConsumeQueue offset
            long currentOffset = flatFile.getConsumeQueueMaxOffset();
            // 已经刷盘的 ConsumeQueue offset
            long commitOffset = flatFile.getConsumeQueueCommitOffset();
            long minOffsetInQueue = defaultStore.getMinOffsetInQueue(topic, queueId);
            long maxOffsetInQueue = defaultStore.getMaxOffsetInQueue(topic, queueId);

            // 如果 ConsumeQueue 的 FileSegment 文件完全没有初始化，则初始化文件
            // If set to max offset here, some written messages may be lost
            if (!flatFile.isFlatFileInit()) {
                currentOffset = Math.max(minOffsetInQueue,
                    maxOffsetInQueue - storeConfig.getTieredStoreGroupCommitSize());
                flatFile.initOffset(currentOffset);
                return CompletableFuture.completedFuture(true);
            }

            // 如果上一次刷盘失败（已刷盘 offset 小于提交到缓冲区的 offset，说明没有全部刷盘成功），立即重试上次刷盘
            // If the previous commit fails, attempt to trigger a commit directly.
            if (commitOffset < currentOffset) {
                this.commitAsync(flatFile);
                return CompletableFuture.completedFuture(false);
            }

            // 如果当前 offset 小于最小 offset，则销毁文件，重新创建文件
            if (currentOffset < minOffsetInQueue) {
                log.warn("MessageDispatcher#dispatch, current offset is too small, " +
                        "topic={}, queueId={}, offset={}-{}, current={}",
                    topic, queueId, minOffsetInQueue, maxOffsetInQueue, currentOffset);
                flatFileStore.destroyFile(flatFile.getMessageQueue());
                flatFileStore.computeIfAbsent(new MessageQueue(topic, brokerName, queueId));
                return CompletableFuture.completedFuture(true);
            }

            if (currentOffset > maxOffsetInQueue) {
                log.warn("MessageDispatcher#dispatch, current offset is too large, " +
                        "topic: {}, queueId: {}, offset={}-{}, current={}",
                    topic, queueId, minOffsetInQueue, maxOffsetInQueue, currentOffset);
                return CompletableFuture.completedFuture(false);
            }

            // 如果超过滚动时间（24h），则滚动文件
            long interval = TimeUnit.HOURS.toMillis(storeConfig.getCommitLogRollingInterval());
            if (flatFile.rollingFile(interval)) {
                log.info("MessageDispatcher#dispatch, rolling file, " +
                        "topic: {}, queueId: {}, offset={}-{}, current={}",
                    topic, queueId, minOffsetInQueue, maxOffsetInQueue, currentOffset);
            }

            if (currentOffset == maxOffsetInQueue) {
                return CompletableFuture.completedFuture(false);
            }

            long bufferSize = 0L;
            long groupCommitSize = storeConfig.getTieredStoreGroupCommitSize();
            long groupCommitCount = storeConfig.getTieredStoreGroupCommitCount();
            // 计算目标 offset，为当前以提交到缓冲区的 ConsumeQueue offset 加上单次提交的消息数阈值
            long targetOffset = Math.min(currentOffset + groupCommitCount, maxOffsetInQueue);

            // 判断是否需要立即提交，还是继续攒批
            // 取出最后 append 到缓冲区的一条消息
            ConsumeQueueInterface consumeQueue = defaultStore.getConsumeQueue(topic, queueId);
            CqUnit cqUnit = consumeQueue.get(currentOffset);
            SelectMappedBufferResult message =
                defaultStore.selectOneMessageByOffset(cqUnit.getPos(), cqUnit.getSize());
            // 超时：上次提交到当前时间是否超过分级存储存储的提交时间阈值（30s）
            boolean timeout = MessageFormatUtil.getStoreTimeStamp(message.getByteBuffer()) +
                storeConfig.getTieredStoreGroupCommitTimeout() < System.currentTimeMillis();
            // 缓冲区满：当前队列等待提交的消息数量超过阈值（4096）
            boolean bufferFull = maxOffsetInQueue - currentOffset > storeConfig.getTieredStoreGroupCommitCount();

            if (!timeout && !bufferFull && !force) {
                // 如果没有到提交时间阈值、缓冲区没有满、没有强制刷盘，则不进行刷盘，继续攒批
                log.debug("MessageDispatcher#dispatch hold, topic={}, queueId={}, offset={}-{}, current={}, remain={}",
                    topic, queueId, minOffsetInQueue, maxOffsetInQueue, currentOffset, maxOffsetInQueue - currentOffset);
                return CompletableFuture.completedFuture(false);
            } else {
                // 如果到提交时间阈值或者缓冲区满或者强制刷盘，则进行刷盘
                if (MessageFormatUtil.getStoreTimeStamp(message.getByteBuffer()) +
                    TimeUnit.MINUTES.toMillis(5) < System.currentTimeMillis()) {
                    log.warn("MessageDispatcher#dispatch behind too much, topic={}, queueId={}, offset={}-{}, current={}, remain={}",
                        topic, queueId, minOffsetInQueue, maxOffsetInQueue, currentOffset, maxOffsetInQueue - currentOffset);
                } else {
                    log.info("MessageDispatcher#dispatch, topic={}, queueId={}, offset={}-{}, current={}, remain={}",
                        topic, queueId, minOffsetInQueue, maxOffsetInQueue, currentOffset, maxOffsetInQueue - currentOffset);
                }
            }
            message.release();

            // 准备提交，先将消息放入缓冲区
            // 对于目标偏移量之前的每个偏移量，从消费队列中获取消费队列单元，然后根据其从本地存储中查询消息
            // 将消息追加到 CommitLog 缓冲区，并将分发请求追加到 ConsumeQueue 缓冲区
            long offset = currentOffset;
            for (; offset < targetOffset; offset++) {
                cqUnit = consumeQueue.get(offset);
                bufferSize += cqUnit.getSize();
                if (bufferSize >= groupCommitSize) {
                    break;
                }
                message = defaultStore.selectOneMessageByOffset(cqUnit.getPos(), cqUnit.getSize());

                // 将消息追加到分级存储 CommitLog 缓冲区
                ByteBuffer byteBuffer = message.getByteBuffer();
                AppendResult result = flatFile.appendCommitLog(message);
                if (!AppendResult.SUCCESS.equals(result)) {
                    break;
                }

                long mappedCommitLogOffset = flatFile.getCommitLogMaxOffset() - byteBuffer.remaining();
                Map<String, String> properties = MessageFormatUtil.getProperties(byteBuffer);

                DispatchRequest dispatchRequest = new DispatchRequest(topic, queueId, mappedCommitLogOffset,
                    cqUnit.getSize(), cqUnit.getTagsCode(), MessageFormatUtil.getStoreTimeStamp(byteBuffer),
                    cqUnit.getQueueOffset(), properties.getOrDefault(MessageConst.PROPERTY_KEYS, ""),
                    properties.getOrDefault(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, ""),
                    0, 0, new HashMap<>());
                dispatchRequest.setOffsetId(MessageFormatUtil.getOffsetId(byteBuffer));

                // 提交一个 DispatchRequest 到分级存储 ConsumeQueue
                result = flatFile.appendConsumeQueue(dispatchRequest);
                if (!AppendResult.SUCCESS.equals(result)) {
                    break;
                }
            }

            // 如果等待提交的消息数量超过阈值（4096），立即进行下一次提交
            // If there are many messages waiting to be uploaded, call the upload logic immediately.
            boolean repeat = timeout || maxOffsetInQueue - offset > storeConfig.getTieredStoreGroupCommitCount();

            // 如果 FlatMessageFile 中待分发的 ConsumeQueue 请求不为空，则将缓冲区中的数据刷到二级存储
            if (!flatFile.getDispatchRequestList().isEmpty()) {
                Attributes attributes = TieredStoreMetricsManager.newAttributesBuilder()
                    .put(TieredStoreMetricsConstant.LABEL_TOPIC, topic)
                    .put(TieredStoreMetricsConstant.LABEL_QUEUE_ID, queueId)
                    .put(TieredStoreMetricsConstant.LABEL_FILE_TYPE, FileSegmentType.COMMIT_LOG.name().toLowerCase())
                    .build();
                TieredStoreMetricsManager.messagesDispatchTotal.add(offset - currentOffset, attributes);

                this.commitAsync(flatFile).whenComplete((unused, throwable) -> {
                        if (repeat) {
                            // 如果等待提交的消息数量超过阈值（4096），立即进行下一次提交
                            storeExecutor.commonExecutor.submit(() -> dispatchWithSemaphore(flatFile));
                        }
                    }
                );
            }
        } finally {
            flatFile.getFileLock().unlock();
        }
        return CompletableFuture.completedFuture(false);
    }

    /**
     * 执行 CommitLog 刷盘，再执行 ConsumeQueue 的刷盘，再执行 Index 构建（如果开启 Index）
     *
     * @param flatFile
     * @return
     */
    public CompletableFuture<Void> commitAsync(FlatFileInterface flatFile) {
        return flatFile.commitAsync().thenAcceptAsync(success -> {
            if (success) {
                if (storeConfig.isMessageIndexEnable()) {
                    flatFile.getDispatchRequestList().forEach(
                        request -> constructIndexFile(flatFile.getTopicId(), request));
                }
                flatFile.release();
            }
        }, MessageStoreExecutor.getInstance().bufferCommitExecutor);
    }

    /**
     * 根据分发请求构建 Index
     * Building indexes with offsetId is no longer supported because offsetId has changed in tiered storage
     */
    public void constructIndexFile(long topicId, DispatchRequest request) {
        Set<String> keySet = new HashSet<>();
        if (StringUtils.isNotBlank(request.getUniqKey())) {
            keySet.add(request.getUniqKey());
        }
        if (StringUtils.isNotBlank(request.getKeys())) {
            keySet.addAll(Arrays.asList(request.getKeys().split(MessageConst.KEY_SEPARATOR)));
        }
        indexService.putKey(request.getTopic(), (int) topicId, request.getQueueId(), keySet,
            request.getCommitLogOffset(), request.getMsgSize(), request.getStoreTimestamp());
    }

    /**
     * 定时任务，每隔 20 秒为每个队列执行一次分发
     */
    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        while (!this.isStopped()) {
            flatFileStore.deepCopyFlatFileToList().forEach(this::dispatchWithSemaphore);
            this.waitForRunning(Duration.ofSeconds(20).toMillis());
        }
        log.info("{} service shutdown", this.getServiceName());
    }
}
