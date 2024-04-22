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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.file.FlatAppendFile;
import org.apache.rocketmq.tieredstore.file.FlatFileFactory;
import org.apache.rocketmq.tieredstore.provider.FileSegment;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 分级存储索引文件服务
 */
public class IndexStoreService extends ServiceThread implements IndexService {

    private static final Logger log = LoggerFactory.getLogger(MessageStoreUtil.TIERED_STORE_LOGGER_NAME);

    public static final String FILE_DIRECTORY_NAME = "tiered_index_file";
    public static final String FILE_COMPACTED_DIRECTORY_NAME = "compacting";

    /**
     * File status in table example:
     * upload, upload, upload, sealed, sealed, unsealed
     */
    private final MessageStoreConfig storeConfig;
    /**
     * 索引文件表，根据创建时间排序的跳表
     */
    private final ConcurrentSkipListMap<Long /* timestamp */, IndexFile> timeStoreTable;
    private final ReadWriteLock readWriteLock;
    private final AtomicLong compactTimestamp;
    private final String filePath;
    /**
     * FlatAppendFile 工厂，用于创建 IndexFile 的 FlatAppendFile。这里只在 recover 时使用
     */
    private final FlatFileFactory fileAllocator;

    /**
     * 正在写入的索引文件，也是 {@link #timeStoreTable} 中的最后一个索引文件
     */
    private IndexFile currentWriteFile;
    private FlatAppendFile flatAppendFile;

    public IndexStoreService(FlatFileFactory flatFileFactory, String filePath) {
        this.storeConfig = flatFileFactory.getStoreConfig();
        this.filePath = filePath;
        this.fileAllocator = flatFileFactory;
        this.timeStoreTable = new ConcurrentSkipListMap<>();
        this.compactTimestamp = new AtomicLong(0L);
        this.readWriteLock = new ReentrantReadWriteLock();
        this.recover();
    }

    private void doConvertOldFormatFile(String filePath) {
        try {
            File file = new File(filePath);
            if (!file.exists()) {
                return;
            }
            MappedFile mappedFile = new DefaultMappedFile(file.getPath(), (int) file.length());
            long timestamp = mappedFile.getMappedByteBuffer().getLong(IndexStoreFile.INDEX_BEGIN_TIME_STAMP);
            if (timestamp <= 0) {
                mappedFile.destroy(TimeUnit.SECONDS.toMillis(10));
            } else {
                mappedFile.renameTo(String.valueOf(new File(file.getParent(), String.valueOf(timestamp))));
                mappedFile.shutdown(TimeUnit.SECONDS.toMillis(10));
            }
        } catch (Exception e) {
            log.error("IndexStoreService do convert old format error, file: {}", filePath, e);
        }
    }

    private void recover() {
        Stopwatch stopwatch = Stopwatch.createStarted();

        // 删除已经压缩但没有上传的本地索引文件
        // delete compact file directory
        UtilAll.deleteFile(new File(Paths.get(storeConfig.getStorePathRootDir(),
            FILE_DIRECTORY_NAME, FILE_COMPACTED_DIRECTORY_NAME).toString()));

        // 恢复本地旧的没有上传的索引文件
        // recover local
        File dir = new File(Paths.get(storeConfig.getStorePathRootDir(), FILE_DIRECTORY_NAME).toString());
        this.doConvertOldFormatFile(Paths.get(dir.getPath(), "0000").toString());
        this.doConvertOldFormatFile(Paths.get(dir.getPath(), "1111").toString());
        File[] files = dir.listFiles();

        if (files != null) {
            List<File> fileList = Arrays.asList(files);
            fileList.sort(Comparator.comparing(File::getName));

            for (File file : fileList) {
                if (file.isDirectory() || !StringUtils.isNumeric(file.getName())) {
                    continue;
                }

                try {
                    IndexFile indexFile = new IndexStoreFile(storeConfig, Long.parseLong(file.getName()));
                    timeStoreTable.put(indexFile.getTimestamp(), indexFile);
                    log.info("IndexStoreService recover load local file, timestamp: {}", indexFile.getTimestamp());
                } catch (Exception e) {
                    log.error("IndexStoreService recover, load local file error", e);
                }
            }
        }

        if (this.timeStoreTable.isEmpty()) {
            this.createNewIndexFile(System.currentTimeMillis());
        }

        this.currentWriteFile = this.timeStoreTable.lastEntry().getValue();
        this.setCompactTimestamp(this.timeStoreTable.firstKey() - 1);

        // 从元数据恢复已经上传到分级存储的索引文件句柄
        // recover remote
        this.flatAppendFile = fileAllocator.createFlatFileForIndexFile(filePath);

        // 为每个分级存储的 FileSegment 创建 IndexFile
        for (FileSegment fileSegment : flatAppendFile.getFileSegmentList()) {
            IndexFile indexFile = new IndexStoreFile(storeConfig, fileSegment);
            IndexFile localFile = timeStoreTable.get(indexFile.getTimestamp());
            if (localFile != null) {
                localFile.destroy();
            }
            timeStoreTable.put(indexFile.getTimestamp(), indexFile);
            log.info("IndexStoreService recover load remote file, timestamp: {}, end timestamp: {}",
                indexFile.getTimestamp(), indexFile.getEndTimestamp());
        }

        log.info("IndexStoreService recover finished, total: {}, cost: {}ms, directory: {}",
            timeStoreTable.size(), stopwatch.elapsed(TimeUnit.MILLISECONDS), dir.getAbsolutePath());
    }

    public void createNewIndexFile(long timestamp) {
        try {
            this.readWriteLock.writeLock().lock();
            IndexFile indexFile = this.currentWriteFile;
            if (this.timeStoreTable.containsKey(timestamp) ||
                indexFile != null && IndexFile.IndexStatusEnum.UNSEALED.equals(indexFile.getFileStatus())) {
                return;
            }
            IndexStoreFile newStoreFile = new IndexStoreFile(storeConfig, timestamp);
            this.timeStoreTable.put(timestamp, newStoreFile);
            this.currentWriteFile = newStoreFile;
            log.info("IndexStoreService construct next file, timestamp: {}", timestamp);
        } catch (Exception e) {
            log.error("IndexStoreService construct next file, timestamp: {}", timestamp, e);
        } finally {
            this.readWriteLock.writeLock().unlock();
        }
    }

    @VisibleForTesting
    public ConcurrentSkipListMap<Long, IndexFile> getTimeStoreTable() {
        return timeStoreTable;
    }

    /**
     * 向最新的索引文件中写入索引项
     *
     * @param topic     The topic of the key.
     * @param topicId   The ID of the topic.
     * @param queueId   The ID of the queue.
     * @param keySet    The set of keys to be indexed.
     * @param offset    The offset value of the key.
     * @param size      The size of the key.
     * @param timestamp The timestamp of the key.
     * @return
     */
    @Override
    public AppendResult putKey(
        String topic, int topicId, int queueId, Set<String> keySet, long offset, int size, long timestamp) {

        if (StringUtils.isBlank(topic)) {
            return AppendResult.UNKNOWN_ERROR;
        }

        if (keySet == null || keySet.isEmpty()) {
            return AppendResult.SUCCESS;
        }

        // 向当前写入的索引文件中写入索引项，重试 3 次
        for (int i = 0; i < 3; i++) {
            AppendResult result = this.currentWriteFile.putKey(
                topic, topicId, queueId, keySet, offset, size, timestamp);

            if (AppendResult.SUCCESS.equals(result)) {
                return AppendResult.SUCCESS;
            } else if (AppendResult.FILE_FULL.equals(result)) {
                // 当前索引文件已满，创建新的索引文件
                // use current time to ensure the order of file
                this.createNewIndexFile(System.currentTimeMillis());
            }
        }

        // 写入失败
        log.error("IndexStoreService put key three times return error, topic: {}, topicId: {}, " +
            "queueId: {}, keySize: {}, timestamp: {}", topic, topicId, queueId, keySet.size(), timestamp);
        return AppendResult.UNKNOWN_ERROR;
    }

    /**
     * 异步查询索引项
     *
     * @param topic     The topic of the key.
     * @param key       The key to be queried.
     * @param maxCount
     * @param beginTime The start time of the query range.
     * @param endTime   The end time of the query range.
     * @return
     */
    @Override
    public CompletableFuture<List<IndexItem>> queryAsync(
        String topic, String key, int maxCount, long beginTime, long endTime) {

        CompletableFuture<List<IndexItem>> future = new CompletableFuture<>();
        try {
            readWriteLock.readLock().lock();
            // 获取时间范围内的所有索引文件
            ConcurrentNavigableMap<Long, IndexFile> pendingMap =
                this.timeStoreTable.subMap(beginTime, true, endTime, true);
            List<CompletableFuture<Void>> futureList = new ArrayList<>(pendingMap.size());
            ConcurrentHashMap<String /* queueId-offset */, IndexItem> result = new ConcurrentHashMap<>();

            // 逆序遍历索引文件，异步查询索引项
            for (Map.Entry<Long, IndexFile> entry : pendingMap.descendingMap().entrySet()) {
                CompletableFuture<Void> completableFuture = entry.getValue()
                    .queryAsync(topic, key, maxCount, beginTime, endTime)
                    .thenAccept(itemList -> itemList.forEach(indexItem -> {
                        if (result.size() < maxCount) {
                            result.put(String.format(
                                "%d-%d", indexItem.getQueueId(), indexItem.getOffset()), indexItem);
                        }
                    }));
                futureList.add(completableFuture);
            }

            // 等待所有查询任务完成
            CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0]))
                .whenComplete((v, t) -> {
                    // Try to return the query results as much as possible here
                    // rather than directly throwing exceptions
                    if (result.isEmpty() && t != null) {
                        future.completeExceptionally(t);
                    } else {
                        List<IndexItem> resultList = new ArrayList<>(result.values());
                        future.complete(resultList.subList(0, Math.min(resultList.size(), maxCount)));
                    }
                });
        } catch (Exception e) {
            future.completeExceptionally(e);
        } finally {
            readWriteLock.readLock().unlock();
        }
        return future;
    }

    /**
     * 压缩索引文件并上传到二级存储
     *
     * @param indexFile
     * @return
     */
    public boolean doCompactThenUploadFile(IndexFile indexFile) {
        if (IndexFile.IndexStatusEnum.UPLOAD.equals(indexFile.getFileStatus())) {
            log.error("IndexStoreService file status not correct, so skip, timestamp: {}, status: {}",
                indexFile.getTimestamp(), indexFile.getFileStatus());
            indexFile.destroy();
            return true;
        }

        Stopwatch stopwatch = Stopwatch.createStarted();
        // 如果缓冲区的所有内容都已刷盘到二级存储，则可以进行压缩
        if (flatAppendFile.getCommitOffset() == flatAppendFile.getAppendOffset()) {
            // 压缩成新索引文件，返回新文件的 ByteBuffer
            ByteBuffer byteBuffer = indexFile.doCompaction();
            if (byteBuffer == null) {
                log.error("IndexStoreService found compaction buffer is null, timestamp: {}", indexFile.getTimestamp());
                return false;
            }
            // 创建新的 FileSegment，即压缩后的索引文件
            flatAppendFile.rollingNewFile(Math.max(0L, flatAppendFile.getAppendOffset()));
            flatAppendFile.append(byteBuffer, indexFile.getTimestamp());
            flatAppendFile.getFileToWrite().setMinTimestamp(indexFile.getTimestamp());
            flatAppendFile.getFileToWrite().setMaxTimestamp(indexFile.getEndTimestamp());
        }
        // 等待压缩后的索引文件刷盘到分级存储
        boolean result = flatAppendFile.commitAsync().join();

        List<FileSegment> fileSegmentList = flatAppendFile.getFileSegmentList();
        FileSegment fileSegment = fileSegmentList.get(fileSegmentList.size() - 1);
        if (!result || fileSegment == null || fileSegment.getMinTimestamp() != indexFile.getTimestamp()) {
            log.warn("IndexStoreService upload compacted file error, timestamp: {}", indexFile.getTimestamp());
            return false;
        } else {
            log.info("IndexStoreService upload compacted file success, timestamp: {}", indexFile.getTimestamp());
        }

        // 将上传后的所以你文件封装成 IndexFile，保存到 timeStoreTable 中
        readWriteLock.writeLock().lock();
        try {
            IndexFile storeFile = new IndexStoreFile(storeConfig, fileSegment);
            timeStoreTable.put(storeFile.getTimestamp(), storeFile);
            // 删除本地 IndexFile（未压缩的和压缩后的）
            indexFile.destroy();
        } catch (Exception e) {
            log.error("IndexStoreService rolling file error, timestamp: {}, cost: {}ms",
                indexFile.getTimestamp(), stopwatch.elapsed(TimeUnit.MILLISECONDS), e);
        } finally {
            readWriteLock.writeLock().unlock();
        }
        return true;
    }

    public void destroyExpiredFile(long expireTimestamp) {
        // delete file in time store table
        readWriteLock.writeLock().lock();
        try {
            timeStoreTable.entrySet().removeIf(entry ->
                entry.getKey() < expireTimestamp &&
                    IndexFile.IndexStatusEnum.UPLOAD.equals(entry.getValue().getFileStatus()));
            flatAppendFile.destroyExpiredFile(expireTimestamp);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public void destroy() {
        readWriteLock.writeLock().lock();
        try {
            // delete local store file
            for (Map.Entry<Long, IndexFile> entry : timeStoreTable.entrySet()) {
                IndexFile indexFile = entry.getValue();
                if (IndexFile.IndexStatusEnum.UPLOAD.equals(indexFile.getFileStatus())) {
                    continue;
                }
                indexFile.destroy();
            }
            // delete remote
            if (flatAppendFile != null) {
                flatAppendFile.destroy();
            }
        } catch (Exception e) {
            log.error("IndexStoreService destroy all file error", e);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public String getServiceName() {
        return IndexStoreService.class.getSimpleName();
    }

    public void setCompactTimestamp(long timestamp) {
        this.compactTimestamp.set(timestamp);
        log.debug("IndexStoreService set compact timestamp to: {}", timestamp);
    }

    /**
     * 按时间顺序找到下一个待压缩的索引文件
     * <p>
     * 根据 {@link #compactTimestamp} 找到下一个 的索引文件，并且不是最后一个文件。一般只有最后一个文件是 UNSEALED 状态。
     *
     * @return 下一个待压缩的索引文件
     */
    protected IndexFile getNextSealedFile() {
        Map.Entry<Long, IndexFile> entry =
            this.timeStoreTable.higherEntry(this.compactTimestamp.get());
        if (entry != null && entry.getKey() < this.timeStoreTable.lastKey()) {
            return entry.getValue();
        }
        return null;
    }

    @Override
    public void shutdown() {
        super.shutdown();
        readWriteLock.writeLock().lock();
        try {
            for (Map.Entry<Long /* timestamp */, IndexFile> entry : timeStoreTable.entrySet()) {
                entry.getValue().shutdown();
            }
            this.timeStoreTable.clear();
        } catch (Exception e) {
            log.error("IndexStoreService shutdown error", e);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    /**
     * 每 10s 进行一次扫描和压缩
     */
    @Override
    public void run() {
        while (!this.isStopped()) {
            // 删除过期索引文件
            long expireTimestamp = System.currentTimeMillis()
                - TimeUnit.HOURS.toMillis(storeConfig.getTieredStoreFileReservedTime());
            this.destroyExpiredFile(expireTimestamp);

            // 按时间顺序找到下一个 SEALED 待压缩文件
            IndexFile indexFile = this.getNextSealedFile();
            // 压缩并上传
            if (indexFile != null) {
                if (this.doCompactThenUploadFile(indexFile)) {
                    this.setCompactTimestamp(indexFile.getTimestamp());
                    continue;
                }
            }
            this.waitForRunning(TimeUnit.SECONDS.toMillis(10));
        }
        log.info(this.getServiceName() + " service shutdown");
    }
}
