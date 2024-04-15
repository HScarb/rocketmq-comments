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
package org.apache.rocketmq.tieredstore.index;

import com.google.common.base.Stopwatch;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.MessageStoreExecutor;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.provider.FileSegment;
import org.apache.rocketmq.tieredstore.provider.PosixFileSegment;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.rocketmq.tieredstore.index.IndexFile.IndexStatusEnum.SEALED;
import static org.apache.rocketmq.tieredstore.index.IndexFile.IndexStatusEnum.UNSEALED;
import static org.apache.rocketmq.tieredstore.index.IndexFile.IndexStatusEnum.UPLOAD;
import static org.apache.rocketmq.tieredstore.index.IndexItem.COMPACT_INDEX_ITEM_SIZE;
import static org.apache.rocketmq.tieredstore.index.IndexStoreService.FILE_COMPACTED_DIRECTORY_NAME;
import static org.apache.rocketmq.tieredstore.index.IndexStoreService.FILE_DIRECTORY_NAME;

/**
 * 分级存储索引文件
 * a single IndexFile in indexService
 */
public class IndexStoreFile implements IndexFile {

    private static final Logger log = LoggerFactory.getLogger(MessageStoreUtil.TIERED_STORE_LOGGER_NAME);

    /**
     * header format:
     * magic code(4) + begin timestamp(8) + end timestamp(8) + slot num(4) + index num(4)
     */
    public static final int INDEX_MAGIC_CODE = 0;
    public static final int INDEX_BEGIN_TIME_STAMP = 4;
    public static final int INDEX_END_TIME_STAMP = 12;
    public static final int INDEX_SLOT_COUNT = 20;
    public static final int INDEX_ITEM_INDEX = 24;
    public static final int INDEX_HEADER_SIZE = 28;

    public static final int BEGIN_MAGIC_CODE = 0xCCDDEEFF ^ 1880681586 + 4;
    public static final int END_MAGIC_CODE = 0xCCDDEEFF ^ 1880681586 + 8;

    /**
     * hash slot
     */
    private static final int INVALID_INDEX = 0;
    private static final int HASH_SLOT_SIZE = Long.BYTES;
    private static final int MAX_QUERY_COUNT = 512;

    /**
     * 最大哈希槽数，默认 500w
     */
    private final int hashSlotMaxCount;
    /**
     * 最大索引项数，默认 2000w
     */
    private final int indexItemMaxCount;

    private final ReadWriteLock fileReadWriteLock;
    /**
     * 索引文件状态
     */
    private final AtomicReference<IndexStatusEnum> fileStatus;
    private final AtomicLong beginTimestamp = new AtomicLong(-1L);
    private final AtomicLong endTimestamp = new AtomicLong(-1L);
    private final AtomicInteger hashSlotCount = new AtomicInteger(0);
    private final AtomicInteger indexItemCount = new AtomicInteger(0);

    /**
     * {@link org.apache.rocketmq.tieredstore.index.IndexFile.IndexStatusEnum#UNSEALED} 状态下的索引文件，
     * 类似本地 {@link org.apache.rocketmq.store.index.IndexFile} 的格式
     */
    private MappedFile mappedFile;
    private ByteBuffer byteBuffer;

    /**
     * {@link org.apache.rocketmq.tieredstore.index.IndexFile.IndexStatusEnum#SEALED} 状态下的索引文件，
     * 正在或已经压缩，等待上传到二级存储
     */
    private MappedFile compactMappedFile;

    /**
     * {@link org.apache.rocketmq.tieredstore.index.IndexFile.IndexStatusEnum#UPLOAD} 状态下的索引文件，已上传二级存储
     */
    private FileSegment fileSegment;

    /**
     * 创建 {@link org.apache.rocketmq.tieredstore.index.IndexFile.IndexStatusEnum#UNSEALED} 状态的索引文件
     *
     * @param storeConfig
     * @param timestamp
     * @throws IOException
     */
    public IndexStoreFile(MessageStoreConfig storeConfig, long timestamp) throws IOException {
        this.hashSlotMaxCount = storeConfig.getTieredStoreIndexFileMaxHashSlotNum();
        this.indexItemMaxCount = storeConfig.getTieredStoreIndexFileMaxIndexNum();
        this.fileStatus = new AtomicReference<>(UNSEALED);
        this.fileReadWriteLock = new ReentrantReadWriteLock();
        this.mappedFile = new DefaultMappedFile(
            Paths.get(storeConfig.getStorePathRootDir(), FILE_DIRECTORY_NAME, String.valueOf(timestamp)).toString(),
            this.getItemPosition(indexItemMaxCount));
        this.byteBuffer = this.mappedFile.getMappedByteBuffer();

        this.beginTimestamp.set(timestamp);
        this.endTimestamp.set(byteBuffer.getLong(INDEX_BEGIN_TIME_STAMP));
        this.hashSlotCount.set(byteBuffer.getInt(INDEX_SLOT_COUNT));
        this.indexItemCount.set(byteBuffer.getInt(INDEX_ITEM_INDEX));
        this.flushNewMetadata(byteBuffer, indexItemMaxCount == this.indexItemCount.get() + 1);
    }

    /**
     * 创建 {@link org.apache.rocketmq.tieredstore.index.IndexFile.IndexStatusEnum#UPLOAD} 状态的索引文件
     * 用刷盘到二级存储的索引文件初始化 IndexFile
     *
     * @param storeConfig
     * @param fileSegment 压缩后的索引文件上传到二级存储得到的 FileSegment
     */
    public IndexStoreFile(MessageStoreConfig storeConfig, FileSegment fileSegment) {
        this.fileSegment = fileSegment;
        this.fileStatus = new AtomicReference<>(UPLOAD);
        this.fileReadWriteLock = new ReentrantReadWriteLock();

        this.beginTimestamp.set(fileSegment.getMinTimestamp());
        this.endTimestamp.set(fileSegment.getMaxTimestamp());
        this.hashSlotCount.set(storeConfig.getTieredStoreIndexFileMaxHashSlotNum());
        this.indexItemCount.set(storeConfig.getTieredStoreIndexFileMaxIndexNum());
        this.hashSlotMaxCount = hashSlotCount.get();
        this.indexItemMaxCount = indexItemCount.get();
    }

    @Override
    public long getTimestamp() {
        return this.beginTimestamp.get();
    }

    @Override
    public long getEndTimestamp() {
        return this.endTimestamp.get();
    }

    public long getHashSlotCount() {
        return this.hashSlotCount.get();
    }

    public long getIndexItemCount() {
        return this.indexItemCount.get();
    }

    @Override
    public IndexStatusEnum getFileStatus() {
        return this.fileStatus.get();
    }

    protected String buildKey(String topic, String key) {
        return String.format("%s#%s", topic, key);
    }

    protected int hashCode(String keyStr) {
        int keyHash = keyStr.hashCode();
        return (keyHash < 0) ? -keyHash : keyHash;
    }

    /**
     * 更新索引文件 Header
     *
     * @param byteBuffer
     * @param end
     */
    protected void flushNewMetadata(ByteBuffer byteBuffer, boolean end) {
        byteBuffer.putInt(INDEX_MAGIC_CODE, !end ? BEGIN_MAGIC_CODE : END_MAGIC_CODE);
        byteBuffer.putLong(INDEX_BEGIN_TIME_STAMP, this.beginTimestamp.get());
        byteBuffer.putLong(INDEX_END_TIME_STAMP, this.endTimestamp.get());
        byteBuffer.putInt(INDEX_SLOT_COUNT, this.hashSlotCount.get());
        byteBuffer.putInt(INDEX_ITEM_INDEX, this.indexItemCount.get());
    }

    protected int getSlotPosition(int slotIndex) {
        return INDEX_HEADER_SIZE + slotIndex * HASH_SLOT_SIZE;
    }

    protected int getSlotValue(int slotPosition) {
        return Math.max(this.byteBuffer.getInt(slotPosition), INVALID_INDEX);
    }

    protected int getItemPosition(int itemIndex) {
        return INDEX_HEADER_SIZE + hashSlotMaxCount * HASH_SLOT_SIZE + itemIndex * IndexItem.INDEX_ITEM_SIZE;
    }

    @Override
    public void start() {

    }

    @Override
    public AppendResult putKey(
        String topic, int topicId, int queueId, Set<String> keySet, long offset, int size, long timestamp) {

        if (StringUtils.isBlank(topic)) {
            return AppendResult.UNKNOWN_ERROR;
        }

        if (keySet == null || keySet.isEmpty()) {
            return AppendResult.SUCCESS;
        }

        try {
            fileReadWriteLock.writeLock().lock();

            // 只有 UNSEALED 状态的索引文件才允许被写入
            if (!UNSEALED.equals(fileStatus.get())) {
                return AppendResult.FILE_FULL;
            }

            // 索引数量超过最大值（默认 2000w），将索引文件状态置为 SEALED 等待压缩，返回文件已满
            if (this.indexItemCount.get() + keySet.size() >= this.indexItemMaxCount) {
                this.fileStatus.set(IndexStatusEnum.SEALED);
                return AppendResult.FILE_FULL;
            }

            // 遍历每个 Key，插入索引项
            for (String key : keySet) {
                int hashCode = this.hashCode(this.buildKey(topic, key));
                int slotPosition = this.getSlotPosition(hashCode % this.hashSlotMaxCount);
                int slotOldValue = this.getSlotValue(slotPosition);
                int timeDiff = (int) ((timestamp - this.beginTimestamp.get()) / 1000L);

                // 构造 IndexItem，写入索引文件
                IndexItem indexItem = new IndexItem(
                    topicId, queueId, offset, size, hashCode, timeDiff, slotOldValue);
                int itemIndex = this.indexItemCount.incrementAndGet();
                this.byteBuffer.position(this.getItemPosition(itemIndex));
                this.byteBuffer.put(indexItem.getByteBuffer());
                this.byteBuffer.putInt(slotPosition, itemIndex);

                if (slotOldValue <= INVALID_INDEX) {
                    this.hashSlotCount.incrementAndGet();
                }
                // 更新 endTimestamp
                if (this.endTimestamp.get() < timestamp) {
                    this.endTimestamp.set(timestamp);
                }
                // 更新索引文件 Header
                this.flushNewMetadata(byteBuffer, indexItemMaxCount == this.indexItemCount.get() + 1);

                log.trace("IndexStoreFile put key, timestamp: {}, topic: {}, key: {}, slot: {}, item: {}, previous item: {}, content: {}",
                    this.getTimestamp(), topic, key, hashCode % this.hashSlotMaxCount, itemIndex, slotOldValue, indexItem);
            }
            return AppendResult.SUCCESS;
        } catch (Exception e) {
            log.error("IndexStoreFile put key error, topic: {}, topicId: {}, queueId: {}, keySet: {}, offset: {}, " +
                "size: {}, timestamp: {}", topic, topicId, queueId, keySet, offset, size, timestamp, e);
        } finally {
            fileReadWriteLock.writeLock().unlock();
        }

        return AppendResult.UNKNOWN_ERROR;
    }

    @Override
    public CompletableFuture<List<IndexItem>> queryAsync(
        String topic, String key, int maxCount, long beginTime, long endTime) {

        switch (this.fileStatus.get()) {
            case UNSEALED:
            case SEALED:
                // 从本地未压缩的索引文件中查询索引项。SEALED 状态的索引文件仍然会保留未压缩前的索引文件。
                return this.queryAsyncFromUnsealedFile(buildKey(topic, key), maxCount, beginTime, endTime);
            case UPLOAD:
                // 从已压缩并上传到二级存储的索引文件中查询索引项
                return this.queryAsyncFromSegmentFile(buildKey(topic, key), maxCount, beginTime, endTime);
            case SHUTDOWN:
            default:
                return CompletableFuture.completedFuture(new ArrayList<>());
        }
    }

    /**
     * 从未压缩的索引文件中查询索引项。SEALED 状态的索引文件仍然会保留未压缩前的索引文件，可能已经创建新索引文件并正在压缩
     *
     * @param key
     * @param maxCount
     * @param beginTime
     * @param endTime
     * @return
     */
    protected CompletableFuture<List<IndexItem>> queryAsyncFromUnsealedFile(
        String key, int maxCount, long beginTime, long endTime) {

        return CompletableFuture.supplyAsync(() -> {
            List<IndexItem> result = new ArrayList<>();
            try {
                fileReadWriteLock.readLock().lock();
                if (!UNSEALED.equals(this.fileStatus.get()) && !SEALED.equals(this.fileStatus.get())) {
                    return result;
                }

                if (mappedFile == null || !mappedFile.hold()) {
                    return result;
                }

                // 根据 key 的 hashCode 计算 hash 槽位置，获取 hash 槽的值。它指向第一个索引项的位置
                int hashCode = this.hashCode(key);
                int slotPosition = this.getSlotPosition(hashCode % this.hashSlotMaxCount);
                int slotValue = this.getSlotValue(slotPosition);

                // 遍历索引项链表，直到找到足够的索引项或者达到最大查询次数（默认 512）
                int left = MAX_QUERY_COUNT;
                while (left > 0 &&
                    slotValue > INVALID_INDEX &&
                    slotValue <= this.indexItemCount.get()) {

                    byte[] bytes = new byte[IndexItem.INDEX_ITEM_SIZE];
                    ByteBuffer buffer = this.byteBuffer.duplicate();
                    buffer.position(this.getItemPosition(slotValue));
                    buffer.get(bytes);
                    IndexItem indexItem = new IndexItem(bytes);
                    if (hashCode == indexItem.getHashCode()) {
                        result.add(indexItem);
                        if (result.size() > maxCount) {
                            break;
                        }
                    }
                    slotValue = indexItem.getItemIndex();
                    left--;
                }

                log.debug("IndexStoreFile query from unsealed mapped file, timestamp: {}, result size: {}, " +
                        "key: {}, hashCode: {}, maxCount: {}, timestamp={}-{}",
                    getTimestamp(), result.size(), key, hashCode, maxCount, beginTime, endTime);
            } catch (Exception e) {
                log.error("IndexStoreFile query from unsealed mapped file error, timestamp: {}, " +
                    "key: {}, maxCount: {}, timestamp={}-{}", getTimestamp(), key, maxCount, beginTime, endTime, e);
            } finally {
                fileReadWriteLock.readLock().unlock();
                mappedFile.release();
            }
            return result;
        }, MessageStoreExecutor.getInstance().bufferFetchExecutor);
    }

    /**
     * 从已压缩并上传到二级存储的索引文件中查询索引项
     *
     * @param key
     * @param maxCount
     * @param beginTime
     * @param endTime
     * @return
     */
    protected CompletableFuture<List<IndexItem>> queryAsyncFromSegmentFile(
        String key, int maxCount, long beginTime, long endTime) {

        if (this.fileSegment == null || !UPLOAD.equals(this.fileStatus.get())) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        Stopwatch stopwatch = Stopwatch.createStarted();
        // 从二级存储中读取索引文件，根据 key 的 hashCode 计算 hash 槽位置
        int hashCode = this.hashCode(key);
        int slotPosition = this.getSlotPosition(hashCode % this.hashSlotMaxCount);

        // 根据 hash 槽位置查询 hash 槽
        CompletableFuture<List<IndexItem>> future = this.fileSegment.readAsync(slotPosition, HASH_SLOT_SIZE)
            .thenCompose(slotBuffer -> {
                if (slotBuffer.remaining() < HASH_SLOT_SIZE) {
                    log.error("IndexStoreFile query from tiered storage return error slot buffer, " +
                        "key: {}, maxCount: {}, timestamp={}-{}", key, maxCount, beginTime, endTime);
                    return CompletableFuture.completedFuture(null);
                }
                // 读取 hash 槽中的索引项起始位置和总长度
                int indexPosition = slotBuffer.getInt();
                int indexTotalSize = Math.min(slotBuffer.getInt(), COMPACT_INDEX_ITEM_SIZE * 1024);
                if (indexPosition <= INVALID_INDEX || indexTotalSize <= 0) {
                    return CompletableFuture.completedFuture(null);
                }
                // 根据索引项起始位置和索引项总长度读取索引项
                return this.fileSegment.readAsync(indexPosition, indexTotalSize);
            })
            // 组装读取到的索引项
            .thenApply(itemBuffer -> {
                List<IndexItem> result = new ArrayList<>();
                if (itemBuffer == null) {
                    return result;
                }

                if (itemBuffer.remaining() % COMPACT_INDEX_ITEM_SIZE != 0) {
                    log.error("IndexStoreFile query from tiered storage return error item buffer, " +
                        "key: {}, maxCount: {}, timestamp={}-{}", key, maxCount, beginTime, endTime);
                    return result;
                }

                // 遍历索引项，根据索引项的时间戳范围和 hashCode 过滤索引项，直到找到足够的索引项
                int size = itemBuffer.remaining() / COMPACT_INDEX_ITEM_SIZE;
                byte[] bytes = new byte[COMPACT_INDEX_ITEM_SIZE];
                for (int i = 0; i < size; i++) {
                    itemBuffer.get(bytes);
                    IndexItem indexItem = new IndexItem(bytes);
                    long storeTimestamp = indexItem.getTimeDiff() + beginTimestamp.get();
                    if (hashCode == indexItem.getHashCode() &&
                        beginTime <= storeTimestamp && storeTimestamp <= endTime &&
                        result.size() < maxCount) {
                        result.add(indexItem);
                    }
                }
                return result;
            });

        return future.whenComplete((result, throwable) -> {
            long costTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            if (throwable != null) {
                log.error("IndexStoreFile query from segment file, cost: {}ms, timestamp: {}, " +
                        "key: {}, hashCode: {}, maxCount: {}, timestamp={}-{}",
                    costTime, getTimestamp(), key, hashCode, maxCount, beginTime, endTime, throwable);
            } else {
                String details = Optional.ofNullable(result)
                    .map(r -> r.stream()
                        .map(item -> String.format("%d-%d", item.getQueueId(), item.getOffset()))
                        .collect(Collectors.joining(", ")))
                    .orElse("");

                log.debug("IndexStoreFile query from segment file, cost: {}ms, timestamp: {}, result size: {}, ({}), " +
                        "key: {}, hashCode: {}, maxCount: {}, timestamp={}-{}",
                    costTime, getTimestamp(), result != null ? result.size() : 0, details, key, hashCode, maxCount, beginTime, endTime);
            }
        });
    }

    /**
     * 压缩索引文件到新文件，设置索引文件状态为 SEALED，返回新文件 ByteBuffer
     *
     * @return 压缩后索引文件 ByteBuffer，读模式
     */
    @Override
    public ByteBuffer doCompaction() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        ByteBuffer buffer;
        try {
            buffer = compactToNewFile();
            log.debug("IndexStoreFile do compaction, timestamp: {}, file size: {}, cost: {}ms",
                this.getTimestamp(), buffer.capacity(), stopwatch.elapsed(TimeUnit.MICROSECONDS));
        } catch (Exception e) {
            log.error("IndexStoreFile do compaction, timestamp: {}, cost: {}ms",
                this.getTimestamp(), stopwatch.elapsed(TimeUnit.MICROSECONDS), e);
            return null;
        }

        try {
            // Make sure there is no read request here
            fileReadWriteLock.writeLock().lock();
            fileStatus.set(IndexStatusEnum.SEALED);
        } catch (Exception e) {
            log.error("IndexStoreFile change file status to sealed error, timestamp={}", this.getTimestamp());
        } finally {
            fileReadWriteLock.writeLock().unlock();
        }
        return buffer;
    }

    protected String getCompactedFilePath() {
        return Paths.get(this.mappedFile.getFileName()).getParent()
            .resolve(FILE_COMPACTED_DIRECTORY_NAME)
            .resolve(String.valueOf(this.getTimestamp())).toString();
    }

    /**
     * 将 UNSEALED 状态的索引文件压缩到新文件
     * <p>
     * 压缩文件于压缩前文件相比
     * <ul>
     *     <li>header 不变</li>
     *     <li>hash 槽从 4byte 扩大到 8byte，增加了</li>
     *     <li>索引项经过排序，去掉了指针，从 32byte 变为 28byte</li>
     * </ul>
     *
     * @return 压缩后的新文件 ByteBuffer，读模式
     * @throws IOException
     */
    protected ByteBuffer compactToNewFile() throws IOException {

        byte[] payload = new byte[IndexItem.INDEX_ITEM_SIZE];
        ByteBuffer payloadBuffer = ByteBuffer.wrap(payload);
        // 索引项开始写入位置 = header size + hash 槽总 size（hash 槽数 500w * hash 槽 size 8）
        int writePosition = INDEX_HEADER_SIZE + (hashSlotMaxCount * HASH_SLOT_SIZE);
        // 文件大小 = 索引项写入位置 + 索引项总 size（索引项数 2000w * 索引项 size 32）
        int fileMaxLength = writePosition + COMPACT_INDEX_ITEM_SIZE * indexItemCount.get();

        // 创建新的压缩索引文件
        compactMappedFile = new DefaultMappedFile(this.getCompactedFilePath(), fileMaxLength);
        // 压缩后的索引文件 ByteBuffer
        MappedByteBuffer newBuffer = compactMappedFile.getMappedByteBuffer();

        // 遍历所有 hash 槽（500w）
        for (int i = 0; i < hashSlotMaxCount; i++) {
            int slotPosition = this.getSlotPosition(i);
            int slotValue = this.getSlotValue(slotPosition);
            int writeBeginPosition = writePosition;

            // 遍历 hash 槽中所有的索引项
            while (slotValue > INVALID_INDEX && writePosition < fileMaxLength) {
                // 读取压缩前索引项
                ByteBuffer buffer = this.byteBuffer.duplicate();
                buffer.position(this.getItemPosition(slotValue));
                buffer.get(payload);
                // 读取老索引项的下一个索引项位置
                int newSlotValue = payloadBuffer.getInt(COMPACT_INDEX_ITEM_SIZE);
                // 截掉老索引项的下一个索引项位置，新索引项中不需要。这行是多余的，后面没有操作 buffer
                buffer.limit(COMPACT_INDEX_ITEM_SIZE);
                // 索引项写入到新索引文件
                newBuffer.position(writePosition);
                newBuffer.put(payload, 0, COMPACT_INDEX_ITEM_SIZE);
                log.trace("IndexStoreFile do compaction, write item, slot: {}, current: {}, next: {}", i, slotValue, newSlotValue);
                // 指向下一个索引项位置
                slotValue = newSlotValue;
                // 写入位置后移
                writePosition += COMPACT_INDEX_ITEM_SIZE;
            }

            // 计算所有索引项总长度
            int length = writePosition - writeBeginPosition;
            // 向压缩后的文件写入 hash 槽数据
            // 0~4byte: 这个 hash 槽索引项的起始位置
            newBuffer.putInt(slotPosition, writeBeginPosition);
            // 5~8byte: 这个 hash 槽所有索引项的总长度
            newBuffer.putInt(slotPosition + Integer.BYTES, length);

            if (length > 0) {
                log.trace("IndexStoreFile do compaction, write slot, slot: {}, begin: {}, length: {}", i, writeBeginPosition, length);
            }
        }

        // 更新 header
        this.flushNewMetadata(newBuffer, true);
        // 切换成读模式
        newBuffer.flip();
        return newBuffer;
    }

    @Override
    public void shutdown() {
        try {
            fileReadWriteLock.writeLock().lock();
            this.fileStatus.set(IndexStatusEnum.SHUTDOWN);
            if (this.fileSegment != null && this.fileSegment instanceof PosixFileSegment) {
                ((PosixFileSegment) this.fileSegment).close();
            }
            if (this.mappedFile != null) {
                this.mappedFile.shutdown(TimeUnit.SECONDS.toMillis(10));
            }
            if (this.compactMappedFile != null) {
                this.compactMappedFile.shutdown(TimeUnit.SECONDS.toMillis(10));
            }
        } catch (Exception e) {
            log.error("IndexStoreFile shutdown failed, timestamp: {}, status: {}", this.getTimestamp(), fileStatus.get(), e);
        } finally {
            fileReadWriteLock.writeLock().unlock();
        }
    }

    @Override
    public void destroy() {
        try {
            fileReadWriteLock.writeLock().lock();
            this.shutdown();
            switch (this.fileStatus.get()) {
                case SHUTDOWN:
                case UNSEALED:
                case SEALED:
                    if (this.mappedFile != null) {
                        this.mappedFile.destroy(TimeUnit.SECONDS.toMillis(10));
                    }
                    if (this.compactMappedFile != null) {
                        this.compactMappedFile.destroy(TimeUnit.SECONDS.toMillis(10));
                    }
                    log.debug("IndexStoreService destroy local file, timestamp: {}, status: {}", this.getTimestamp(), fileStatus.get());
                    break;
                case UPLOAD:
                    log.warn("[BUG] IndexStoreService destroy remote file, timestamp: {}", this.getTimestamp());
            }
        } catch (Exception e) {
            log.error("IndexStoreService destroy failed, timestamp: {}, status: {}", this.getTimestamp(), fileStatus.get(), e);
        } finally {
            fileReadWriteLock.writeLock().unlock();
        }
    }
}
