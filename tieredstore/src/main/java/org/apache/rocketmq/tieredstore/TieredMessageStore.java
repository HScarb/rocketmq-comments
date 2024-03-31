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
package org.apache.rocketmq.tieredstore;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;

import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.QueryMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.plugin.AbstractPluginMessageStore;
import org.apache.rocketmq.store.plugin.MessageStorePluginContext;
import org.apache.rocketmq.tieredstore.core.MessageStoreDispatcher;
import org.apache.rocketmq.tieredstore.core.MessageStoreDispatcherImpl;
import org.apache.rocketmq.tieredstore.core.MessageStoreFetcher;
import org.apache.rocketmq.tieredstore.core.MessageStoreFetcherImpl;
import org.apache.rocketmq.tieredstore.core.MessageStoreFilter;
import org.apache.rocketmq.tieredstore.core.MessageStoreTopicFilter;
import org.apache.rocketmq.tieredstore.file.FlatFileStore;
import org.apache.rocketmq.tieredstore.file.FlatMessageFile;
import org.apache.rocketmq.tieredstore.index.IndexService;
import org.apache.rocketmq.tieredstore.index.IndexStoreService;
import org.apache.rocketmq.tieredstore.metadata.MetadataStore;
import org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant;
import org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsManager;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.ViewBuilder;

public class TieredMessageStore extends AbstractPluginMessageStore {

    protected static final Logger log = LoggerFactory.getLogger(MessageStoreUtil.TIERED_STORE_LOGGER_NAME);

    protected final String brokerName;

    /**
     * 默认 MessageStore 的引用，当前有
     * {@link org.apache.rocketmq.store.DefaultMessageStore} 和
     * {@link org.apache.rocketmq.store.RocksDBMessageStore} 两个实现
     */
    protected final MessageStore defaultStore;
    protected final MessageStoreConfig storeConfig;
    protected final MessageStorePluginContext context;

    /**
     * 分级存储元数据存储实现
     */
    protected final MetadataStore metadataStore;
    protected final MessageStoreExecutor storeExecutor;
    protected final IndexService indexService;

    /**
     * 分级存储的文件存储实现
     */
    protected final FlatFileStore flatFileStore;
    protected final MessageStoreFilter topicFilter;

    /**
     * 消息拉取器，用于从分级存储中拉取消息
     */
    protected final MessageStoreFetcher fetcher;

    /**
     * 消息分发器，用于将消息写入分级存储
     */
    protected final MessageStoreDispatcher dispatcher;

    public TieredMessageStore(MessageStorePluginContext context, MessageStore next) {
        super(context, next);

        this.storeConfig = new MessageStoreConfig();
        this.context = context;
        // 从配置文件读取存储配置
        this.context.registerConfiguration(this.storeConfig);
        this.brokerName = this.storeConfig.getBrokerName();
        this.defaultStore = next;

        this.metadataStore = this.getMetadataStore(this.storeConfig);
        this.topicFilter = new MessageStoreTopicFilter(this.storeConfig);
        this.storeExecutor = new MessageStoreExecutor();
        this.flatFileStore = new FlatFileStore(this.storeConfig, this.metadataStore, this.storeExecutor);
        this.indexService = new IndexStoreService(this.flatFileStore.getFlatFileFactory(),
            MessageStoreUtil.getIndexFilePath(this.storeConfig.getBrokerName()));
        this.fetcher = new MessageStoreFetcherImpl(this);
        // 创建分级存储分发器实例，添加到 CommitLog 消息分发器列表中
        this.dispatcher = new MessageStoreDispatcherImpl(this);
        next.addDispatcher(dispatcher);
    }

    /**
     * 重新加载分级存储
     */
    @Override
    public boolean load() {
        boolean loadFlatFile = flatFileStore.load();
        boolean loadNextStore = next.load();
        boolean result = loadFlatFile && loadNextStore;
        // 如果加载成功，启动索引服务和分级存储消息上传线程
        if (result) {
            indexService.start();
            dispatcher.start();
            storeExecutor.commonExecutor.scheduleWithFixedDelay(
                flatFileStore::scheduleDeleteExpireFile, storeConfig.getTieredStoreDeleteFileInterval(),
                storeConfig.getTieredStoreDeleteFileInterval(), TimeUnit.MILLISECONDS);
        }
        return result;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public MessageStoreConfig getStoreConfig() {
        return storeConfig;
    }

    public MessageStore getDefaultStore() {
        return defaultStore;
    }

    private MetadataStore getMetadataStore(MessageStoreConfig storeConfig) {
        try {
            Class<? extends MetadataStore> clazz =
                Class.forName(storeConfig.getTieredMetadataServiceProvider()).asSubclass(MetadataStore.class);
            Constructor<? extends MetadataStore> constructor = clazz.getConstructor(MessageStoreConfig.class);
            return constructor.newInstance(storeConfig);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public MetadataStore getMetadataStore() {
        return metadataStore;
    }

    public MessageStoreFilter getTopicFilter() {
        return topicFilter;
    }

    public MessageStoreExecutor getStoreExecutor() {
        return storeExecutor;
    }

    public FlatFileStore getFlatFileStore() {
        return flatFileStore;
    }

    public IndexService getIndexService() {
        return indexService;
    }

    public boolean fetchFromCurrentStore(String topic, int queueId, long offset) {
        return fetchFromCurrentStore(topic, queueId, offset, 1);
    }

    /**
     * 判断是否要从分层存储中 fetch 消息
     *
     * @param topic
     * @param queueId
     * @param offset
     * @param batchSize
     * @return
     */
    @SuppressWarnings("all")
    public boolean fetchFromCurrentStore(String topic, int queueId, long offset, int batchSize) {
        MessageStoreConfig.TieredStorageLevel storageLevel = storeConfig.getTieredStorageLevel();

        // FORCE - 强制走分级存储
        if (storageLevel.check(MessageStoreConfig.TieredStorageLevel.FORCE)) {
            return true;
        }

        // DISABLE - 禁用分级存储
        if (!storageLevel.isEnable()) {
            return false;
        }

        // 分级存储中没有该队列
        FlatMessageFile flatFile = flatFileStore.getFlatFile(new MessageQueue(topic, brokerName, queueId));
        if (flatFile == null) {
            return false;
        }

        // offset 超出分级存储已提交的 offset
        if (offset >= flatFile.getConsumeQueueCommitOffset()) {
            return false;
        }

        // NOT_IN_DISK（默认），且 offset 不在磁盘中，走分级存储
        // determine whether tiered storage path conditions are met
        if (storageLevel.check(MessageStoreConfig.TieredStorageLevel.NOT_IN_DISK)) {
            // return true to read from tiered storage if the CommitLog is empty
            if (next != null && next.getCommitLog() != null &&
                next.getCommitLog().getMinOffset() < 0L) {
                return true;
            }
            if (!next.checkInStoreByConsumeOffset(topic, queueId, offset)) {
                return true;
            }
        }

        // NOT_IN_MEM，且 offset 不在内存（Page Cache）中，走分级存储
        if (storageLevel.check(MessageStoreConfig.TieredStorageLevel.NOT_IN_MEM)
            && !next.checkInMemByConsumeOffset(topic, queueId, offset, batchSize)) {
            return true;
        }
        return false;
    }

    @Override
    public GetMessageResult getMessage(String group, String topic, int queueId, long offset, int maxMsgNums,
        MessageFilter messageFilter) {
        return getMessageAsync(group, topic, queueId, offset, maxMsgNums, messageFilter).join();
    }

    @Override
    public CompletableFuture<GetMessageResult> getMessageAsync(String group, String topic,
        int queueId, long offset, int maxMsgNums, MessageFilter messageFilter) {

        // 系统 Topic，走本地存储
        // for system topic, force reading from local store
        if (topicFilter.filterTopic(topic)) {
            return next.getMessageAsync(group, topic, queueId, offset, maxMsgNums, messageFilter);
        }

        // 根据 TieredStorageLevel 决定读本地存储还是分级存储，默认 NOT_IN_DISK
        if (fetchFromCurrentStore(topic, queueId, offset, maxMsgNums)) {
            log.trace("GetMessageAsync from remote store, " +
                "topic: {}, queue: {}, offset: {}, maxCount: {}", topic, queueId, offset, maxMsgNums);
        } else {
            log.trace("GetMessageAsync from next store, " +
                "topic: {}, queue: {}, offset: {}, maxCount: {}", topic, queueId, offset, maxMsgNums);
            return next.getMessageAsync(group, topic, queueId, offset, maxMsgNums, messageFilter);
        }

        Stopwatch stopwatch = Stopwatch.createStarted();
        return fetcher
            // 从分级存储 fetch 消息
            .getMessageAsync(group, topic, queueId, offset, maxMsgNums, messageFilter)
            // 处理 fetch 结果
            .thenApply(result -> {
                Attributes latencyAttributes = TieredStoreMetricsManager.newAttributesBuilder()
                    .put(TieredStoreMetricsConstant.LABEL_OPERATION, TieredStoreMetricsConstant.OPERATION_API_GET_MESSAGE)
                    .put(TieredStoreMetricsConstant.LABEL_TOPIC, topic)
                    .put(TieredStoreMetricsConstant.LABEL_GROUP, group)
                    .build();
                TieredStoreMetricsManager.apiLatency.record(stopwatch.elapsed(TimeUnit.MILLISECONDS), latencyAttributes);

                // 如果 fetch 不到（结果为 OFFSET_FOUND_NULL 或 NO_MATCHED_LOGIC_QUEUE），则尝试从本地存储读取
                if (result.getStatus() == GetMessageStatus.OFFSET_FOUND_NULL ||
                    result.getStatus() == GetMessageStatus.NO_MATCHED_LOGIC_QUEUE) {

                    if (next.checkInStoreByConsumeOffset(topic, queueId, offset)) {
                        // 记录监控信息和日志
                        TieredStoreMetricsManager.fallbackTotal.add(1, latencyAttributes);
                        log.debug("GetMessageAsync not found, then back to next store, result: {}, " +
                                "topic: {}, queue: {}, queue offset: {}, offset range: {}-{}",
                            result.getStatus(), topic, queueId, offset, result.getMinOffset(), result.getMaxOffset());
                        // 从本地存储获取
                        return next.getMessage(group, topic, queueId, offset, maxMsgNums, messageFilter);
                    }
                }

                // 消息不在分级存储也不在本地存储
                if (result.getStatus() != GetMessageStatus.FOUND &&
                    result.getStatus() != GetMessageStatus.NO_MESSAGE_IN_QUEUE &&
                    result.getStatus() != GetMessageStatus.NO_MATCHED_LOGIC_QUEUE &&
                    result.getStatus() != GetMessageStatus.OFFSET_TOO_SMALL &&
                    result.getStatus() != GetMessageStatus.OFFSET_OVERFLOW_ONE &&
                    result.getStatus() != GetMessageStatus.OFFSET_OVERFLOW_BADLY) {
                    log.warn("GetMessageAsync not found and message is not in next store, result: {}, " +
                            "topic: {}, queue: {}, queue offset: {}, offset range: {}-{}",
                        result.getStatus(), topic, queueId, offset, result.getMinOffset(), result.getMaxOffset());
                }

                // 找到消息
                if (result.getStatus() == GetMessageStatus.FOUND) {
                    Attributes messagesOutAttributes = TieredStoreMetricsManager.newAttributesBuilder()
                        .put(TieredStoreMetricsConstant.LABEL_TOPIC, topic)
                        .put(TieredStoreMetricsConstant.LABEL_GROUP, group)
                        .build();
                    // 记录监控信息
                    TieredStoreMetricsManager.messagesOutTotal.add(result.getMessageCount(), messagesOutAttributes);

                    if (next.getStoreStatsService() != null) {
                        next.getStoreStatsService().getGetMessageTransferredMsgCount().add(result.getMessageCount());
                    }
                }

                // 用本地存储的队列 minOffset 修正结果中队列的 minOffset
                // Fix min or max offset according next store at last
                long minOffsetInQueue = next.getMinOffsetInQueue(topic, queueId);
                if (minOffsetInQueue >= 0 && minOffsetInQueue < result.getMinOffset()) {
                    result.setMinOffset(minOffsetInQueue);
                }

                // 一般来说，本地 ConsumeQueue 的 offset 稍大于读取消息时的 Commit offset，
                // 所以这里没有必要将 max offset 更新为本地 ConsumeQueue 的偏移量，
                // 否则，下次启动时的 offset 超过 commit offset，将会导致重复消费。
                // In general, the local cq offset is slightly greater than the commit offset in read message,
                // so there is no need to update the maximum offset to the local cq offset here,
                // otherwise it will cause repeated consumption after next start offset over commit offset.

                if (storeConfig.isRecordGetMessageResult()) {
                    log.info("GetMessageAsync result, {}, group: {}, topic: {}, queueId: {}, offset: {}, count:{}",
                        result, group, topic, queueId, offset, maxMsgNums);
                }

                return result;
            }).exceptionally(e -> {
                log.error("GetMessageAsync from tiered store failed", e);
                return next.getMessage(group, topic, queueId, offset, maxMsgNums, messageFilter);
            });
    }

    @Override
    public long getMinOffsetInQueue(String topic, int queueId) {
        long minOffsetInNextStore = next.getMinOffsetInQueue(topic, queueId);
        FlatMessageFile flatFile = flatFileStore.getFlatFile(new MessageQueue(topic, brokerName, queueId));
        if (flatFile == null) {
            return minOffsetInNextStore;
        }
        long minOffsetInTieredStore = flatFile.getConsumeQueueMinOffset();
        if (minOffsetInTieredStore < 0) {
            return minOffsetInNextStore;
        }
        return Math.min(minOffsetInNextStore, minOffsetInTieredStore);
    }

    @Override
    public long getEarliestMessageTime(String topic, int queueId) {
        return getEarliestMessageTimeAsync(topic, queueId).join();
    }

    @Override
    public CompletableFuture<Long> getEarliestMessageTimeAsync(String topic, int queueId) {
        long nextEarliestMessageTime = next.getEarliestMessageTime(topic, queueId);
        long finalNextEarliestMessageTime = nextEarliestMessageTime > 0 ? nextEarliestMessageTime : Long.MAX_VALUE;
        Stopwatch stopwatch = Stopwatch.createStarted();
        return fetcher.getEarliestMessageTimeAsync(topic, queueId)
            .thenApply(time -> {
                Attributes latencyAttributes = TieredStoreMetricsManager.newAttributesBuilder()
                    .put(TieredStoreMetricsConstant.LABEL_OPERATION, TieredStoreMetricsConstant.OPERATION_API_GET_EARLIEST_MESSAGE_TIME)
                    .put(TieredStoreMetricsConstant.LABEL_TOPIC, topic)
                    .build();
                TieredStoreMetricsManager.apiLatency.record(stopwatch.elapsed(TimeUnit.MILLISECONDS), latencyAttributes);
                if (time < 0) {
                    log.debug("GetEarliestMessageTimeAsync failed, try to get earliest message time from next store: topic: {}, queue: {}",
                        topic, queueId);
                    return finalNextEarliestMessageTime != Long.MAX_VALUE ? finalNextEarliestMessageTime : -1;
                }
                return Math.min(finalNextEarliestMessageTime, time);
            });
    }

    @Override
    public CompletableFuture<Long> getMessageStoreTimeStampAsync(String topic, int queueId,
        long consumeQueueOffset) {
        if (fetchFromCurrentStore(topic, queueId, consumeQueueOffset)) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            return fetcher.getMessageStoreTimeStampAsync(topic, queueId, consumeQueueOffset)
                .thenApply(time -> {
                    Attributes latencyAttributes = TieredStoreMetricsManager.newAttributesBuilder()
                        .put(TieredStoreMetricsConstant.LABEL_OPERATION,
                                TieredStoreMetricsConstant.OPERATION_API_GET_TIME_BY_OFFSET)
                        .put(TieredStoreMetricsConstant.LABEL_TOPIC, topic)
                        .build();
                    TieredStoreMetricsManager.apiLatency.record(stopwatch.elapsed(TimeUnit.MILLISECONDS), latencyAttributes);
                    if (time == -1) {
                        log.debug("GetEarliestMessageTimeAsync failed, try to get message time from next store, topic: {}, queue: {}, queue offset: {}",
                            topic, queueId, consumeQueueOffset);
                        return next.getMessageStoreTimeStamp(topic, queueId, consumeQueueOffset);
                    }
                    return time;
                });
        }
        return next.getMessageStoreTimeStampAsync(topic, queueId, consumeQueueOffset);
    }

    @Override
    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
        return getOffsetInQueueByTime(topic, queueId, timestamp, BoundaryType.LOWER);
    }

    @Override
    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp, BoundaryType boundaryType) {
        boolean isForce = storeConfig.getTieredStorageLevel() == MessageStoreConfig.TieredStorageLevel.FORCE;
        if (timestamp < next.getEarliestMessageTime() || isForce) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            long offsetInTieredStore = fetcher.getOffsetInQueueByTime(topic, queueId, timestamp, boundaryType);
            Attributes latencyAttributes = TieredStoreMetricsManager.newAttributesBuilder()
                .put(TieredStoreMetricsConstant.LABEL_OPERATION, TieredStoreMetricsConstant.OPERATION_API_GET_OFFSET_BY_TIME)
                .put(TieredStoreMetricsConstant.LABEL_TOPIC, topic)
                .build();
            TieredStoreMetricsManager.apiLatency.record(stopwatch.elapsed(TimeUnit.MILLISECONDS), latencyAttributes);
            if (offsetInTieredStore == -1L && !isForce) {
                return next.getOffsetInQueueByTime(topic, queueId, timestamp);
            }
            return offsetInTieredStore;
        }
        return next.getOffsetInQueueByTime(topic, queueId, timestamp);
    }

    @Override
    public QueryMessageResult queryMessage(String topic, String key, int maxNum, long begin, long end) {
        return queryMessageAsync(topic, key, maxNum, begin, end).join();
    }

    @Override
    public CompletableFuture<QueryMessageResult> queryMessageAsync(String topic, String key,
        int maxNum, long begin, long end) {
        long earliestTimeInNextStore = next.getEarliestMessageTime();
        if (earliestTimeInNextStore <= 0) {
            log.warn("TieredMessageStore#queryMessageAsync: get earliest message time in next store failed: {}", earliestTimeInNextStore);
        }
        boolean isForce = storeConfig.getTieredStorageLevel() == MessageStoreConfig.TieredStorageLevel.FORCE;
        // 如果查询时间在本地存储最早时间之前，或者强制查分级存储，创建一个空的 QueryResult，否则直接查询本地存储
        QueryMessageResult result = end < earliestTimeInNextStore || isForce ?
            new QueryMessageResult() :
            next.queryMessage(topic, key, maxNum, begin, end);
        int resultSize = result.getMessageBufferList().size();
        // 从分级存储查询
        if (resultSize < maxNum && begin < earliestTimeInNextStore || isForce) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            try {
                return fetcher.queryMessageAsync(topic, key, maxNum - resultSize, begin, isForce ? end : earliestTimeInNextStore)
                    .thenApply(tieredStoreResult -> {
                        Attributes latencyAttributes = TieredStoreMetricsManager.newAttributesBuilder()
                            .put(TieredStoreMetricsConstant.LABEL_OPERATION, TieredStoreMetricsConstant.OPERATION_API_QUERY_MESSAGE)
                            .put(TieredStoreMetricsConstant.LABEL_TOPIC, topic)
                            .build();
                        TieredStoreMetricsManager.apiLatency.record(stopwatch.elapsed(TimeUnit.MILLISECONDS), latencyAttributes);
                        for (SelectMappedBufferResult msg : tieredStoreResult.getMessageMapedList()) {
                            result.addMessage(msg);
                        }
                        return result;
                    });
            } catch (Exception e) {
                log.error("TieredMessageStore#queryMessageAsync: query message in tiered store failed", e);
                return CompletableFuture.completedFuture(result);
            }
        }
        return CompletableFuture.completedFuture(result);
    }

    @Override
    public List<Pair<InstrumentSelector, ViewBuilder>> getMetricsView() {
        List<Pair<InstrumentSelector, ViewBuilder>> res = super.getMetricsView();
        res.addAll(TieredStoreMetricsManager.getMetricsView());
        return res;
    }

    @Override
    public void initMetrics(Meter meter, Supplier<AttributesBuilder> attributesBuilderSupplier) {
        super.initMetrics(meter, attributesBuilderSupplier);
        TieredStoreMetricsManager.init(meter, attributesBuilderSupplier, storeConfig, fetcher, flatFileStore, next);
    }

    @Override
    public int cleanUnusedTopic(Set<String> retainTopics) {
        metadataStore.iterateTopic(topicMetadata -> {
            String topic = topicMetadata.getTopic();
            if (retainTopics.contains(topic) ||
                TopicValidator.isSystemTopic(topic) ||
                MixAll.isLmq(topic)) {
                return;
            }
            this.deleteTopics(Sets.newHashSet(topicMetadata.getTopic()));
        });
        return next.cleanUnusedTopic(retainTopics);
    }

    @Override
    public int deleteTopics(Set<String> deleteTopics) {
        for (String topic : deleteTopics) {
            metadataStore.iterateQueue(topic, queueMetadata -> {
                flatFileStore.destroyFile(queueMetadata.getQueue());
            });
            metadataStore.deleteTopic(topic);
            log.info("MessageStore delete topic success, topicName={}", topic);
        }
        return next.deleteTopics(deleteTopics);
    }

    @Override
    public synchronized void shutdown() {
        if (next != null) {
            next.shutdown();
        }
        if (dispatcher != null) {
            dispatcher.shutdown();
        }
        if (indexService != null) {
            indexService.shutdown();
        }
        if (flatFileStore != null) {
            flatFileStore.shutdown();
        }
        if (storeExecutor != null) {
            storeExecutor.shutdown();
        }
    }

    @Override
    public void destroy() {
        if (next != null) {
            next.destroy();
        }
        if (indexService != null) {
            indexService.destroy();
        }
        if (flatFileStore != null) {
            flatFileStore.destroy();
        }
        if (metadataStore != null) {
            metadataStore.destroy();
        }
    }
}
