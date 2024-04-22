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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.QueryMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.TieredMessageStore;
import org.apache.rocketmq.tieredstore.common.GetMessageResultExt;
import org.apache.rocketmq.tieredstore.common.SelectBufferResult;
import org.apache.rocketmq.tieredstore.exception.TieredStoreException;
import org.apache.rocketmq.tieredstore.file.FlatFileStore;
import org.apache.rocketmq.tieredstore.file.FlatMessageFile;
import org.apache.rocketmq.tieredstore.index.IndexItem;
import org.apache.rocketmq.tieredstore.metadata.MetadataStore;
import org.apache.rocketmq.tieredstore.metadata.entity.TopicMetadata;
import org.apache.rocketmq.tieredstore.util.MessageFormatUtil;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 分级存储消息读取器，负责处理分级存储读取请求，包含一层预读缓存
 */
public class MessageStoreFetcherImpl implements MessageStoreFetcher {

    private static final Logger log = LoggerFactory.getLogger(MessageStoreUtil.TIERED_STORE_LOGGER_NAME);

    private static final String CACHE_KEY_FORMAT = "%s@%d@%d";

    private final String brokerName;
    private final MetadataStore metadataStore;
    private final MessageStoreConfig storeConfig;
    private final TieredMessageStore messageStore;
    private final FlatFileStore flatFileStore;
    private final long memoryMaxSize;
    /**
     * 分级存储读取时预读缓存，为了加速从二级存储读取的速度和减少整体上对二级存储请求数
     */
    private final Cache<String /* topic@queueId@offset */, SelectBufferResult> fetcherCache;

    public MessageStoreFetcherImpl(TieredMessageStore messageStore) {
        this.storeConfig = messageStore.getStoreConfig();
        this.brokerName = storeConfig.getBrokerName();
        this.flatFileStore = messageStore.getFlatFileStore();
        this.messageStore = messageStore;
        this.metadataStore = flatFileStore.getMetadataStore();
        // 最大预读缓存大小，为 JVM 最大内存的一定比例，默认 30%
        this.memoryMaxSize =
            (long) (Runtime.getRuntime().maxMemory() * storeConfig.getReadAheadCacheSizeThresholdRate());
        this.fetcherCache = this.initCache(storeConfig);
        log.info("MessageStoreFetcher init success, brokerName={}", storeConfig.getBrokerName());
    }

    private Cache<String, SelectBufferResult> initCache(MessageStoreConfig storeConfig) {

        return Caffeine.newBuilder()
            .scheduler(Scheduler.systemScheduler())
            // 客户端可能会重复请求分层存储中相同偏移量的消息，导致请求队列变满。使用读或写后过期策略刷新缓存过期时间。
            // 读或写后 15s 缓存过期
            // Clients may repeatedly request messages at the same offset in tiered storage,
            // causing the request queue to become full. Using expire after read or write policy
            // to refresh the cache expiration time.
            .expireAfterAccess(storeConfig.getReadAheadCacheExpireDuration(), TimeUnit.MILLISECONDS)
            .maximumWeight(memoryMaxSize)
            // 使用消息的 buffer 大小作为计算内存使用量
            // Using the buffer size of messages to calculate memory usage
            .weigher((String key, SelectBufferResult buffer) -> buffer.getSize())
            // 开启统计
            .recordStats()
            .build();
    }

    public Cache<String, SelectBufferResult> getFetcherCache() {
        return fetcherCache;
    }

    /**
     * 将消息放入预读缓存
     *
     * @param flatFile
     * @param offset queueOffset
     * @param result
     */
    protected void putMessageToCache(FlatMessageFile flatFile, long offset, SelectBufferResult result) {
        MessageQueue mq = flatFile.getMessageQueue();
        this.fetcherCache.put(String.format(CACHE_KEY_FORMAT, mq.getTopic(), mq.getQueueId(), offset), result);
    }

    /**
     * 从分级存储预读缓存中根据逻辑 offset 读取一条消息
     *
     * @param flatFile
     * @param offset queueOffset
     * @return
     */
    protected SelectBufferResult getMessageFromCache(FlatMessageFile flatFile, long offset) {
        MessageQueue mq = flatFile.getMessageQueue();
        SelectBufferResult buffer = this.fetcherCache.getIfPresent(
            String.format(CACHE_KEY_FORMAT, mq.getTopic(), mq.getQueueId(), offset));
        // return duplicate buffer here
        if (buffer == null) {
            return null;
        }
        long count = buffer.getAccessCount().incrementAndGet();
        if (count % 1000L == 0L) {
            log.warn("MessageFetcher fetch same offset message too many times, " +
                "topic={}, queueId={}, offset={}, count={}", mq.getTopic(), mq.getQueueId(), offset, count);
        }
        return new SelectBufferResult(
            buffer.getByteBuffer().asReadOnlyBuffer(), buffer.getStartOffset(), buffer.getSize(), buffer.getTagCode());
    }

    /**
     * 从分级存储预读缓存中一条一条读取消息并拼装
     *
     * @param flatFile
     * @param offset queueOffset
     * @param maxCount
     * @return
     */
    protected GetMessageResultExt getMessageFromCache(FlatMessageFile flatFile, long offset, int maxCount) {
        GetMessageResultExt result = new GetMessageResultExt();
        // 从 queueOffset 开始一条一条读消息，直到读不到消息
        for (long i = offset; i < offset + maxCount; i++) {
            SelectBufferResult buffer = getMessageFromCache(flatFile, i);
            if (buffer == null) {
                break;
            }
            SelectMappedBufferResult bufferResult = new SelectMappedBufferResult(
                buffer.getStartOffset(), buffer.getByteBuffer(), buffer.getSize(), null);
            result.addMessageExt(bufferResult, i, buffer.getTagCode());
        }
        result.setStatus(result.getMessageCount() > 0 ?
            GetMessageStatus.FOUND : GetMessageStatus.OFFSET_OVERFLOW_ONE);
        result.setMinOffset(flatFile.getConsumeQueueMinOffset());
        result.setMaxOffset(flatFile.getConsumeQueueCommitOffset());
        result.setNextBeginOffset(offset + result.getMessageCount());
        return result;
    }

    /**
     * 从二级存储拉消息，放入缓存
     *
     * @param flatFile
     * @param queueOffset
     * @param batchSize
     * @return
     */
    protected CompletableFuture<Long> fetchMessageThenPutToCache(
        FlatMessageFile flatFile, long queueOffset, int batchSize) {

        MessageQueue mq = flatFile.getMessageQueue();
        // 从二级存储读消息
        return this.getMessageFromTieredStoreAsync(flatFile, queueOffset, batchSize)
            .thenApply(result -> {
                if (result.getStatus() == GetMessageStatus.OFFSET_OVERFLOW_ONE ||
                    result.getStatus() == GetMessageStatus.OFFSET_OVERFLOW_BADLY) {
                    return -1L;
                }
                if (result.getStatus() != GetMessageStatus.FOUND) {
                    log.warn("MessageFetcher prefetch message then put to cache failed, result={}, " +
                            "topic={}, queue={}, queue offset={}, batch size={}",
                        result.getStatus(), mq.getTopic(), mq.getQueueId(), queueOffset, batchSize);
                    return -1L;
                }
                List<Long> offsetList = result.getMessageQueueOffset();
                List<Long> tagCodeList = result.getTagCodeList();
                List<SelectMappedBufferResult> msgList = result.getMessageMapedList();
                // 将读到的消息放入缓存
                for (int i = 0; i < offsetList.size(); i++) {
                    SelectMappedBufferResult msg = msgList.get(i);
                    SelectBufferResult bufferResult = new SelectBufferResult(
                        msg.getByteBuffer(), msg.getStartOffset(), msg.getSize(), tagCodeList.get(i));
                    this.putMessageToCache(flatFile, queueOffset + i, bufferResult);
                }
                return offsetList.get(offsetList.size() - 1);
            });
    }

    /**
     * 从分级存储预读缓存读消息
     *
     * @param flatFile
     * @param group
     * @param queueOffset
     * @param maxCount
     * @return
     */
    public CompletableFuture<GetMessageResultExt> getMessageFromCacheAsync(
        FlatMessageFile flatFile, String group, long queueOffset, int maxCount) {

        MessageQueue mq = flatFile.getMessageQueue();
        // 从缓存中读一批消息
        GetMessageResultExt result = getMessageFromCache(flatFile, queueOffset, maxCount);

        // 读取到消息
        if (GetMessageStatus.FOUND.equals(result.getStatus())) {
            log.debug("MessageFetcher cache hit, group={}, topic={}, queueId={}, offset={}, maxCount={}, resultSize={}, lag={}",
                group, mq.getTopic(), mq.getQueueId(), queueOffset, maxCount,
                result.getMessageCount(), result.getMaxOffset() - result.getNextBeginOffset());
            return CompletableFuture.completedFuture(result);
        }

        // 如果缓存中没有读到，立即从二级存储中拉消息，并放入缓存
        // If cache miss, pull messages immediately
        log.debug("MessageFetcher cache miss, group={}, topic={}, queueId={}, offset={}, maxCount={}, lag={}",
            group, mq.getTopic(), mq.getQueueId(), queueOffset, maxCount, result.getMaxOffset() - result.getNextBeginOffset());

        return fetchMessageThenPutToCache(flatFile, queueOffset, storeConfig.getReadAheadMessageCountThreshold())
            .thenApply(maxOffset -> getMessageFromCache(flatFile, queueOffset, maxCount));
    }

    /**
     * 从二级存储中读取消息
     *
     * @param flatFile 分级存储消息队列文件
     * @param queueOffset 要读取消息的偏移量
     * @param batchSize  读取消息的数量
     * @return
     */
    public CompletableFuture<GetMessageResultExt> getMessageFromTieredStoreAsync(
        FlatMessageFile flatFile, long queueOffset, int batchSize) {

        // 从分级存储文件获取最小和最大偏移量，其中最大偏移量取的是消费队列的已提交偏移量（正在上传中的不算在内）
        GetMessageResultExt result = new GetMessageResultExt();
        result.setMinOffset(flatFile.getConsumeQueueMinOffset());
        result.setMaxOffset(flatFile.getConsumeQueueCommitOffset());

        // 根据 fetch 的 queueOffset 和返回结果的 minOffset、maxOffset 来决定返回的结果
        if (queueOffset < result.getMinOffset()) {
            result.setStatus(GetMessageStatus.OFFSET_TOO_SMALL);
            result.setNextBeginOffset(result.getMinOffset());
            return CompletableFuture.completedFuture(result);
        } else if (queueOffset == result.getMaxOffset()) {
            result.setStatus(GetMessageStatus.OFFSET_OVERFLOW_ONE);
            result.setNextBeginOffset(queueOffset);
            return CompletableFuture.completedFuture(result);
        } else if (queueOffset > result.getMaxOffset()) {
            result.setStatus(GetMessageStatus.OFFSET_OVERFLOW_BADLY);
            result.setNextBeginOffset(result.getMaxOffset());
            return CompletableFuture.completedFuture(result);
        }

        if (queueOffset < result.getMaxOffset()) {
            batchSize = Math.min(batchSize, (int) Math.min(
                result.getMaxOffset() - queueOffset, storeConfig.getReadAheadMessageCountThreshold()));
        }

        // 读取 ConsumeQueue
        CompletableFuture<ByteBuffer> readConsumeQueueFuture;
        try {
            readConsumeQueueFuture = flatFile.getConsumeQueueAsync(queueOffset, batchSize);
        } catch (TieredStoreException e) {
            switch (e.getErrorCode()) {
                case ILLEGAL_PARAM:
                case ILLEGAL_OFFSET:
                default:
                    result.setStatus(GetMessageStatus.OFFSET_FOUND_NULL);
                    result.setNextBeginOffset(queueOffset);
                    return CompletableFuture.completedFuture(result);
            }
        }

        int finalBatchSize = batchSize;
        CompletableFuture<ByteBuffer> readCommitLogFuture = readConsumeQueueFuture.thenCompose(cqBuffer -> {

            // 从 ConsumeQueue Buffer 中解析出第一条和最后一条消息的 commitLog offset，并验证是否合法
            long firstCommitLogOffset = MessageFormatUtil.getCommitLogOffsetFromItem(cqBuffer);
            cqBuffer.position(cqBuffer.remaining() - MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE);
            long lastCommitLogOffset = MessageFormatUtil.getCommitLogOffsetFromItem(cqBuffer);
            if (lastCommitLogOffset < firstCommitLogOffset) {
                log.error("MessageFetcher#getMessageFromTieredStoreAsync, last offset is smaller than first offset, " +
                        "topic={} queueId={}, offset={}, firstOffset={}, lastOffset={}",
                    flatFile.getMessageQueue().getTopic(), flatFile.getMessageQueue().getQueueId(), queueOffset,
                    firstCommitLogOffset, lastCommitLogOffset);
                return CompletableFuture.completedFuture(ByteBuffer.allocate(0));
            }

            // 获取整体要读的消息长度，如果长度超过阈值，则缩小单次读取长度（从最后一条消息开始往前缩小，直到缩到只有一条消息）
            // Get at least one message
            // Reducing the length limit of cq to prevent OOM
            long length = lastCommitLogOffset - firstCommitLogOffset + MessageFormatUtil.getSizeFromItem(cqBuffer);
            while (cqBuffer.limit() > MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE &&
                length > storeConfig.getReadAheadMessageSizeThreshold()) {
                cqBuffer.limit(cqBuffer.position());
                cqBuffer.position(cqBuffer.limit() - MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE);
                length = MessageFormatUtil.getCommitLogOffsetFromItem(cqBuffer)
                    - firstCommitLogOffset + MessageFormatUtil.getSizeFromItem(cqBuffer);
            }
            int messageCount = cqBuffer.position() / MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE + 1;

            log.info("MessageFetcher#getMessageFromTieredStoreAsync, " +
                    "topic={}, queueId={}, broker offset={}-{}, offset={}, expect={}, actually={}, lag={}",
                flatFile.getMessageQueue().getTopic(), flatFile.getMessageQueue().getQueueId(),
                result.getMinOffset(), result.getMaxOffset(), queueOffset, finalBatchSize,
                messageCount, result.getMaxOffset() - queueOffset);

            // 从分级存储 CommitLog 中读取消息
            return flatFile.getCommitLogAsync(firstCommitLogOffset, (int) length);
        });

        // 这里用 thenCombine 方法，因为需要使用 cqBuffer 和 msgBuffer 两个入参
        return readConsumeQueueFuture.thenCombine(readCommitLogFuture, (cqBuffer, msgBuffer) -> {
            // 拆分每条消息的 ByteBuffer
            List<SelectBufferResult> bufferList = MessageFormatUtil.splitMessageBuffer(cqBuffer, msgBuffer);
            int requestSize = cqBuffer.remaining() / MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE;

            // not use buffer list size to calculate next offset to prevent split error
            if (bufferList.isEmpty()) {
                // 消息 ByteBuffer 列表为空
                result.setStatus(GetMessageStatus.NO_MATCHED_MESSAGE);
                result.setNextBeginOffset(queueOffset + requestSize);
            } else {
                // 消息 ByteBuffer 列表不为空
                result.setStatus(GetMessageStatus.FOUND);
                result.setNextBeginOffset(queueOffset + requestSize);

                // 将所有消息加入结果
                for (SelectBufferResult bufferResult : bufferList) {
                    ByteBuffer slice = bufferResult.getByteBuffer().slice();
                    slice.limit(bufferResult.getSize());
                    SelectMappedBufferResult msg = new SelectMappedBufferResult(bufferResult.getStartOffset(),
                        bufferResult.getByteBuffer(), bufferResult.getSize(), null);
                    result.addMessageExt(msg, MessageFormatUtil.getQueueOffset(slice), bufferResult.getTagCode());
                }
            }
            return result;
        }).exceptionally(e -> {
            MessageQueue mq = flatFile.getMessageQueue();
            log.warn("MessageFetcher#getMessageFromTieredStoreAsync failed, " +
                "topic={} queueId={}, offset={}, batchSize={}", mq.getTopic(), mq.getQueueId(), queueOffset, finalBatchSize, e);
            result.setStatus(GetMessageStatus.OFFSET_FOUND_NULL);
            result.setNextBeginOffset(queueOffset);
            return result;
        });
    }

    /**
     * 从分级存储读消息
     *
     * @param group         Consumer group that launches this query.
     * @param topic         Topic to query.
     * @param queueId       Queue ID to query.
     * @param queueOffset        Logical offset to start from.
     * @param maxCount      Maximum count of messages to query.
     * @param messageFilter Message filter used to screen desired messages.
     * @return
     */
    @Override
    public CompletableFuture<GetMessageResult> getMessageAsync(
        String group, String topic, int queueId, long queueOffset, int maxCount, final MessageFilter messageFilter) {

        GetMessageResult result = new GetMessageResult();
        // 根据队列查找分级存储文件
        FlatMessageFile flatFile = flatFileStore.getFlatFile(new MessageQueue(topic, brokerName, queueId));

        // 分级存储队列文件不存在，返回 NO_MATCHED_LOGIC_QUEUE
        if (flatFile == null) {
            result.setNextBeginOffset(queueOffset);
            result.setStatus(GetMessageStatus.NO_MATCHED_LOGIC_QUEUE);
            return CompletableFuture.completedFuture(result);
        }

        // 从分级存储文件获取最小和最大偏移量，其中最大偏移量取的是消费队列的已提交偏移量（正在上传中的不算在内）
        // Max queue offset means next message put position
        result.setMinOffset(flatFile.getConsumeQueueMinOffset());
        result.setMaxOffset(flatFile.getConsumeQueueCommitOffset());

        // 根据 fetch 的 queueOffset 和返回结果的 minOffset、maxOffset 来决定返回的结果
        // Fill result according file offset.
        // Offset range  | Result           | Fix to
        // (-oo, 0]      | no message       | current offset
        // (0, min)      | too small        | min offset
        // [min, max)    | correct          |
        // [max, max]    | overflow one     | max offset
        // (max, +oo)    | overflow badly   | max offset

        if (result.getMaxOffset() <= 0) {
            result.setStatus(GetMessageStatus.NO_MESSAGE_IN_QUEUE);
            result.setNextBeginOffset(queueOffset);
            return CompletableFuture.completedFuture(result);
        } else if (queueOffset < result.getMinOffset()) {
            result.setStatus(GetMessageStatus.OFFSET_TOO_SMALL);
            result.setNextBeginOffset(result.getMinOffset());
            return CompletableFuture.completedFuture(result);
        } else if (queueOffset == result.getMaxOffset()) {
            result.setStatus(GetMessageStatus.OFFSET_OVERFLOW_ONE);
            result.setNextBeginOffset(result.getMaxOffset());
            return CompletableFuture.completedFuture(result);
        } else if (queueOffset > result.getMaxOffset()) {
            result.setStatus(GetMessageStatus.OFFSET_OVERFLOW_BADLY);
            result.setNextBeginOffset(result.getMaxOffset());
            return CompletableFuture.completedFuture(result);
        }

        boolean cacheBusy = fetcherCache.estimatedSize() > memoryMaxSize * 0.8;
        if (storeConfig.isReadAheadCacheEnable() && !cacheBusy) {
            // 从缓存读消息
            return getMessageFromCacheAsync(flatFile, group, queueOffset, maxCount)
                .thenApply(messageResultExt -> messageResultExt.doFilterMessage(messageFilter));
        } else {
            // 从分级存储读消息
            return getMessageFromTieredStoreAsync(flatFile, queueOffset, maxCount)
                .thenApply(messageResultExt -> messageResultExt.doFilterMessage(messageFilter));
        }
    }

    @Override
    public CompletableFuture<Long> getEarliestMessageTimeAsync(String topic, int queueId) {
        FlatMessageFile flatFile = flatFileStore.getFlatFile(new MessageQueue(topic, brokerName, queueId));
        if (flatFile == null) {
            return CompletableFuture.completedFuture(-1L);
        }

        // read from timestamp to timestamp + length
        int length = MessageFormatUtil.STORE_TIMESTAMP_POSITION + 8;
        return flatFile.getCommitLogAsync(flatFile.getCommitLogMinOffset(), length)
            .thenApply(MessageFormatUtil::getStoreTimeStamp);
    }

    @Override
    public CompletableFuture<Long> getMessageStoreTimeStampAsync(String topic, int queueId, long queueOffset) {
        FlatMessageFile flatFile = flatFileStore.getFlatFile(new MessageQueue(topic, brokerName, queueId));
        if (flatFile == null) {
            return CompletableFuture.completedFuture(-1L);
        }

        return flatFile.getConsumeQueueAsync(queueOffset)
            .thenComposeAsync(cqItem -> {
                long commitLogOffset = MessageFormatUtil.getCommitLogOffsetFromItem(cqItem);
                int size = MessageFormatUtil.getSizeFromItem(cqItem);
                return flatFile.getCommitLogAsync(commitLogOffset, size);
            }, messageStore.getStoreExecutor().bufferFetchExecutor)
            .thenApply(MessageFormatUtil::getStoreTimeStamp)
            .exceptionally(e -> {
                log.error("MessageStoreFetcherImpl#getMessageStoreTimeStampAsync: " +
                    "get or decode message failed, topic={}, queue={}, offset={}", topic, queueId, queueOffset, e);
                return -1L;
            });
    }

    @Override
    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp, BoundaryType type) {
        FlatMessageFile flatFile = flatFileStore.getFlatFile(new MessageQueue(topic, brokerName, queueId));
        if (flatFile == null) {
            return -1L;
        }
        return flatFile.getQueueOffsetByTimeAsync(timestamp, type).join();
    }

    /**
     * 根据 key 从分级存储索引文件中查询消息
     *
     * @param topic    Topic of the message.
     * @param key      Message key.
     * @param maxCount Maximum count of the messages possible.
     * @param begin    Begin timestamp.
     * @param end      End timestamp.
     * @return
     */
    @Override
    public CompletableFuture<QueryMessageResult> queryMessageAsync(
        String topic, String key, int maxCount, long begin, long end) {

        long topicId;
        try {
            TopicMetadata topicMetadata = metadataStore.getTopic(topic);
            if (topicMetadata == null) {
                log.info("MessageFetcher#queryMessageAsync, topic metadata not found, topic={}", topic);
                return CompletableFuture.completedFuture(new QueryMessageResult());
            }
            topicId = topicMetadata.getTopicId();
        } catch (Exception e) {
            log.error("MessageFetcher#queryMessageAsync, get topic id failed, topic={}", topic, e);
            return CompletableFuture.completedFuture(new QueryMessageResult());
        }

        // 查询分级存储 IndexService，查出索引项
        CompletableFuture<List<IndexItem>> future =
            messageStore.getIndexService().queryAsync(topic, key, maxCount, begin, end);

        return future.thenCompose(indexItemList -> {
            List<CompletableFuture<SelectMappedBufferResult>> futureList = new ArrayList<>(maxCount);
            // 遍历索引项
            for (IndexItem indexItem : indexItemList) {
                if (topicId != indexItem.getTopicId()) {
                    continue;
                }
                // 根据索引项找到 MessageQueue 对应的 FlatMessageFile
                FlatMessageFile flatFile =
                    flatFileStore.getFlatFile(new MessageQueue(topic, brokerName, indexItem.getQueueId()));
                if (flatFile == null) {
                    continue;
                }
                CompletableFuture<SelectMappedBufferResult> getMessageFuture = flatFile
                    .getCommitLogAsync(indexItem.getOffset(), indexItem.getSize())
                    .thenApply(messageBuffer -> new SelectMappedBufferResult(
                        indexItem.getOffset(), messageBuffer, indexItem.getSize(), null));
                futureList.add(getMessageFuture);
                if (futureList.size() >= maxCount) {
                    break;
                }
            }
            // 读取消息完成后将消息加入结果，返回查询结果
            return CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).thenApply(v -> {
                QueryMessageResult result = new QueryMessageResult();
                futureList.forEach(f -> f.thenAccept(result::addMessage));
                return result;
            });
        }).whenComplete((result, throwable) -> {
            if (result != null) {
                log.info("MessageFetcher#queryMessageAsync, " +
                        "query result={}, topic={}, topicId={}, key={}, maxCount={}, timestamp={}-{}",
                    result.getMessageBufferList().size(), topic, topicId, key, maxCount, begin, end);
            }
        });
    }
}
