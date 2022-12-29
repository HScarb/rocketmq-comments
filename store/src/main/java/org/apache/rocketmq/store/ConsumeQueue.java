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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.store.queue.FileQueueLifeCycle;
import org.apache.rocketmq.store.queue.QueueOffsetAssigner;
import org.apache.rocketmq.store.queue.ReferredIterator;

/**
 * 逻辑消费队列实现
 */
public class ConsumeQueue implements ConsumeQueueInterface, FileQueueLifeCycle {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    // 存储单元大小（8+4+8）
    public static final int CQ_STORE_UNIT_SIZE = 20;
    public static final int MSG_TAG_OFFSET_INDEX = 12;
    private static final Logger LOG_ERROR = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    private final MessageStore messageStore;

    private final MappedFileQueue mappedFileQueue;
    private final String topic;
    private final int queueId;
    // 写索引时用到的ByteBuffer
    private final ByteBuffer byteBufferIndex;

    private final String storePath;
    private final int mappedFileSize;
    // 最后一个消息对应的物理Offset
    private long maxPhysicOffset = -1;

    // 逻辑队列的最小Offset，删除物理文件时，计算出来的最小Offset
    // 实际使用需要除以 StoreUnitSize
    /**
     * Minimum offset of the consume file queue that points to valid commit log record.
     */
    private volatile long minLogicOffset = 0;
    private ConsumeQueueExt consumeQueueExt = null;

    public ConsumeQueue(
        final String topic,
        final int queueId,
        final String storePath,
        final int mappedFileSize,
        final MessageStore messageStore) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.messageStore = messageStore;

        this.topic = topic;
        this.queueId = queueId;

        String queueDir = this.storePath
            + File.separator + topic
            + File.separator + queueId;

        this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);

        this.byteBufferIndex = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);

        if (messageStore.getMessageStoreConfig().isEnableConsumeQueueExt()) {
            this.consumeQueueExt = new ConsumeQueueExt(
                topic,
                queueId,
                StorePathConfigHelper.getStorePathConsumeQueueExt(messageStore.getMessageStoreConfig().getStorePathRootDir()),
                messageStore.getMessageStoreConfig().getMappedFileSizeConsumeQueueExt(),
                messageStore.getMessageStoreConfig().getBitMapLengthConsumeQueueExt()
            );
        }
    }

    @Override
    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load consume queue " + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
        if (isExtReadEnable()) {
            result &= this.consumeQueueExt.load();
        }
        return result;
    }

    @Override
    public void recover() {
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // 从倒数第三个文件开始恢复
            int index = mappedFiles.size() - 3;
            if (index < 0) {
                index = 0;
            }

            int mappedFileSizeLogics = this.mappedFileSize;
            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            long maxExtAddr = 1;
            while (true) {
                for (int i = 0; i < mappedFileSizeLogics; i += CQ_STORE_UNIT_SIZE) {
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();

                    // 说明当前存储单元有效
                    if (offset >= 0 && size > 0) {
                        mappedFileOffset = i + CQ_STORE_UNIT_SIZE;
                        this.maxPhysicOffset = offset + size;
                        if (isExtAddr(tagsCode)) {
                            maxExtAddr = tagsCode;
                        }
                    } else {
                        log.info("recover current consume queue file over,  " + mappedFile.getFileName() + " "
                            + offset + " " + size + " " + tagsCode);
                        break;
                    }
                }

                // 走到文件末尾，切换至下一个文件
                if (mappedFileOffset == mappedFileSizeLogics) {
                    index++;
                    if (index >= mappedFiles.size()) {
                        // 当前分支不可能发生
                        log.info("recover last consume queue file over, last mapped file "
                            + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next consume queue file, " + mappedFile.getFileName());
                    }
                } else {
                    log.info("recover current consume queue over " + mappedFile.getFileName() + " "
                        + (processOffset + mappedFileOffset));
                    break;
                }
            }

            // 遍历结束，更新队列文件的Flush和Commit偏移量为遍历到正确消息的偏移量，删除之后的文件
            processOffset += mappedFileOffset;
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            if (isExtReadEnable()) {
                this.consumeQueueExt.recover();
                log.info("Truncate consume queue extend file by max {}", maxExtAddr);
                this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
            }
        }
    }

    @Override
    public long getTotalSize() {
        long totalSize = this.mappedFileQueue.getTotalFileSize();
        if (isExtReadEnable()) {
            totalSize += this.consumeQueueExt.getTotalSize();
        }
        return totalSize;
    }

    @Override
    public int getUnitSize() {
        return CQ_STORE_UNIT_SIZE;
    }

    /**
     * 二分查找查找消息发送时间最接近timestamp逻辑队列的offset
     */
    @Override
    public long getOffsetInQueueByTime(final long timestamp) {
        MappedFile mappedFile = this.mappedFileQueue.getMappedFileByTime(timestamp);
        if (mappedFile != null) {
            long offset = 0;
            // low:第一个索引信息的起始位置
            // minLogicOffset有设置值则从
            // minLogicOffset-mapedFile.getFileFromOffset()位置开始才是有效值
            int low = minLogicOffset > mappedFile.getFileFromOffset() ? (int) (minLogicOffset - mappedFile.getFileFromOffset()) : 0;
            // high:最后一个索引信息的起始位置
            int high = 0;
            int midOffset = -1, targetOffset = -1, leftOffset = -1, rightOffset = -1;
            long leftIndexValue = -1L, rightIndexValue = -1L;
            long minPhysicOffset = this.messageStore.getMinPhyOffset();

            // 取出该mapedFile里面所有的映射空间(没有映射的空间并不会返回,不会返回文件空洞)
            SelectMappedBufferResult sbr = mappedFile.selectMappedBuffer(0);
            if (null != sbr) {
                ByteBuffer byteBuffer = sbr.getByteBuffer();
                high = byteBuffer.limit() - CQ_STORE_UNIT_SIZE;
                try {
                    while (high >= low) {
                        midOffset = (low + high) / (2 * CQ_STORE_UNIT_SIZE) * CQ_STORE_UNIT_SIZE;
                        byteBuffer.position(midOffset);
                        long phyOffset = byteBuffer.getLong();
                        int size = byteBuffer.getInt();
                        if (phyOffset < minPhysicOffset) {
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            continue;
                        }

                        // 比较时间, 折半
                        long storeTime =
                            this.messageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                        if (storeTime < 0) {
                            // 没有从物理文件找到消息，此时直接返回0
                            return 0;
                        } else if (storeTime == timestamp) {
                            targetOffset = midOffset;
                            break;
                        } else if (storeTime > timestamp) {
                            high = midOffset - CQ_STORE_UNIT_SIZE;
                            rightOffset = midOffset;
                            rightIndexValue = storeTime;
                        } else {
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            leftIndexValue = storeTime;
                        }
                    }

                    if (targetOffset != -1) {
                        // 查询的时间正好是消息索引记录写入的时间
                        offset = targetOffset;
                    } else {
                        if (leftIndexValue == -1) {
                            // timestamp 时间小于该MapedFile中第一条记录记录的时间
                            offset = rightOffset;
                        } else if (rightIndexValue == -1) {
                            // timestamp 时间大于该MapedFile中最后一条记录记录的时间
                            offset = leftOffset;
                        } else {
                            // 取最接近timestamp的offset
                            offset =
                                Math.abs(timestamp - leftIndexValue) > Math.abs(timestamp
                                    - rightIndexValue) ? rightOffset : leftOffset;
                        }
                    }

                    return (mappedFile.getFileFromOffset() + offset) / CQ_STORE_UNIT_SIZE;
                } finally {
                    sbr.release();
                }
            }
        }
        // 映射文件被标记为不可用时返回0
        return 0;
    }

    /**
     * 在CommitLog文件恢复时调用，根据CommitLog文件的有效偏移量，删除无效的ConsumeQueue文件
     * @param phyOffset CommitLog文件的有效偏移量
     */
    @Override
    public void truncateDirtyLogicFiles(long phyOffset) {
        truncateDirtyLogicFiles(phyOffset, true);
    }

    public void truncateDirtyLogicFiles(long phyOffset, boolean deleteFile) {

        // ConsumeQueue每个文件大小
        int logicFileSize = this.mappedFileSize;

        // 先改变逻辑队列存储的物理Offset
        this.maxPhysicOffset = phyOffset;
        long maxExtAddr = 1;
        boolean shouldDeleteFile = false;
        while (true) {
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
            if (mappedFile != null) {
                ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
                // 先将Offset清空
                mappedFile.setWrotePosition(0);
                mappedFile.setCommittedPosition(0);
                mappedFile.setFlushedPosition(0);

                for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();
                    // ConsumeQueue起始单元
                    if (0 == i) {
                        if (offset >= phyOffset) {
                            shouldDeleteFile = true;
                            break;
                        } else {
                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.maxPhysicOffset = offset + size;
                            // This maybe not take effect, when not every consume queue has extend file.
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }
                        }
                    } else {    // ConsumeQueue中间单元
                        // 说明当前存储单元有效
                        if (offset >= 0 && size > 0) {
                            // 如果逻辑队列存储的最大物理offset大于物理队列最大offset，则返回
                            if (offset >= phyOffset) {
                                return;
                            }

                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.maxPhysicOffset = offset + size;
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }

                            // 如果最后一个MapedFile扫描完，则返回
                            if (pos == logicFileSize) {
                                return;
                            }
                        } else {
                            return;
                        }
                    }
                }

                if (shouldDeleteFile) {
                    if (deleteFile) {
                        this.mappedFileQueue.deleteLastMappedFile();
                    } else {
                        this.mappedFileQueue.deleteExpiredFile(Collections.singletonList(this.mappedFileQueue.getLastMappedFile()));
                    }
                }

            } else {
                break;
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
        }
    }

    /**
     * 返回最后一条消息对应物理队列的Next Offset
     */
    @Override
    public long getLastOffset() {
        // 物理队列Offset
        long lastOffset = -1;
        // 逻辑队列每个文件大小
        int logicFileSize = this.mappedFileSize;

        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (mappedFile != null) {
            // 找到写入位置对应的索引项的起始位置
            int position = mappedFile.getWrotePosition() - CQ_STORE_UNIT_SIZE;
            if (position < 0)
                position = 0;

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            byteBuffer.position(position);
            for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                long offset = byteBuffer.getLong();
                int size = byteBuffer.getInt();
                byteBuffer.getLong();

                 // 说明当前存储单元有效
                if (offset >= 0 && size > 0) {
                    lastOffset = offset + size;
                } else {
                    break;
                }
            }
        }

        return lastOffset;
    }

    @Override
    public boolean flush(final int flushLeastPages) {
        boolean result = this.mappedFileQueue.flush(flushLeastPages);
        if (isExtReadEnable()) {
            result = result & this.consumeQueueExt.flush(flushLeastPages);
        }

        return result;
    }

    @Override
    public int deleteExpiredFile(long offset) {
        int cnt = this.mappedFileQueue.deleteExpiredFileByOffset(offset, CQ_STORE_UNIT_SIZE);
        // 无论是否删除文件，都需要纠正下最小值，因为有可能物理文件删除了，
        // 但是逻辑文件一个也删除不了
        this.correctMinOffset(offset);
        return cnt;
    }

    /**
     * Update minLogicOffset such that entries after it would point to valid commit log address.
     * 逻辑队列的最小Offset要比传入的物理最小phyMinOffset大
     *
     * @param minCommitLogOffset Minimum commit log offset
     */
    @Override
    public void correctMinOffset(long minCommitLogOffset) {
        // Check if the consume queue is the state of deprecation.
        if (minLogicOffset >= mappedFileQueue.getMaxOffset()) {
            log.info("ConsumeQueue[Topic={}, queue-id={}] contains no valid entries", topic, queueId);
            return;
        }

        // Check whether the consume queue maps no valid data at all. This check may cost 1 IO operation.
        // The rationale is that consume queue always preserves the last file. In case there are many deprecated topics,
        // This check would save a lot of efforts.
        MappedFile lastMappedFile = this.mappedFileQueue.getLastMappedFile();
        if (null == lastMappedFile) {
            return;
        }

        SelectMappedBufferResult lastRecord = null;
        try {
            int maxReadablePosition = lastMappedFile.getReadPosition();
            lastRecord = lastMappedFile.selectMappedBuffer(maxReadablePosition - ConsumeQueue.CQ_STORE_UNIT_SIZE,
                ConsumeQueue.CQ_STORE_UNIT_SIZE);
            if (null != lastRecord) {
                ByteBuffer buffer = lastRecord.getByteBuffer();
                long commitLogOffset = buffer.getLong();
                if (commitLogOffset < minCommitLogOffset) {
                    // Keep the largest known consume offset, even if this consume-queue contains no valid entries at
                    // all. Let minLogicOffset point to a future slot.
                    this.minLogicOffset = lastMappedFile.getFileFromOffset() + maxReadablePosition;
                    log.info("ConsumeQueue[topic={}, queue-id={}] contains no valid entries. Min-offset is assigned as: {}.",
                        topic, queueId, getMinOffsetInQueue());
                    return;
                }
            }
        } finally {
            if (null != lastRecord) {
                lastRecord.release();
            }
        }

        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        long minExtAddr = 1;
        if (mappedFile != null) {
            // Search from previous min logical offset. Typically, a consume queue file segment contains 300,000 entries
            // searching from previous position saves significant amount of comparisons and IOs
            boolean intact = true; // Assume previous value is still valid
            long start = this.minLogicOffset - mappedFile.getFileFromOffset();
            if (start < 0) {
                intact = false;
                start = 0;
            }

            if (start > mappedFile.getReadPosition()) {
                log.error("[Bug][InconsistentState] ConsumeQueue file {} should have been deleted",
                    mappedFile.getFileName());
                return;
            }

            SelectMappedBufferResult result = mappedFile.selectMappedBuffer((int) start);
            if (result == null) {
                log.warn("[Bug] Failed to scan consume queue entries from file on correcting min offset: {}",
                    mappedFile.getFileName());
                return;
            }
            try {
                // No valid consume entries
                if (result.getSize() == 0) {
                    log.debug("ConsumeQueue[topic={}, queue-id={}] contains no valid entries", topic, queueId);
                    return;
                }

                ByteBuffer buffer = result.getByteBuffer().slice();
                // Verify whether the previous value is still valid or not before conducting binary search
                long commitLogOffset = buffer.getLong();
                if (intact && commitLogOffset >= minCommitLogOffset) {
                    log.info("Abort correction as previous min-offset points to {}, which is greater than {}",
                        commitLogOffset, minCommitLogOffset);
                    return;
                }

                // Binary search between range [previous_min_logic_offset, first_file_from_offset + file_size)
                // Note the consume-queue deletion procedure ensures the last entry points to somewhere valid.
                int low = 0;
                int high = result.getSize() - ConsumeQueue.CQ_STORE_UNIT_SIZE;
                while (true) {
                    if (high - low <= ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                        break;
                    }
                    int mid = (low + high) / 2 / ConsumeQueue.CQ_STORE_UNIT_SIZE * ConsumeQueue.CQ_STORE_UNIT_SIZE;
                    buffer.position(mid);
                    commitLogOffset = buffer.getLong();
                    if (commitLogOffset > minCommitLogOffset) {
                        high = mid;
                    } else if (commitLogOffset == minCommitLogOffset) {
                        low = mid;
                        high = mid;
                        break;
                    } else {
                        low = mid;
                    }
                }

                // Examine the last one or two entries
                for (int i = low; i <= high; i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                    buffer.position(i);
                    long offsetPy = buffer.getLong();
                    buffer.position(i + 12);
                    long tagsCode = buffer.getLong();

                    if (offsetPy >= minCommitLogOffset) {
                        this.minLogicOffset = mappedFile.getFileFromOffset() + start + i;
                        log.info("Compute logical min offset: {}, topic: {}, queueId: {}",
                            this.getMinOffsetInQueue(), this.topic, this.queueId);
                        // This maybe not take effect, when not every consume queue has an extended file.
                        if (isExtAddr(tagsCode)) {
                            minExtAddr = tagsCode;
                        }
                        break;
                    }
                }
            } catch (Exception e) {
                log.error("Exception thrown when correctMinOffset", e);
            } finally {
                result.release();
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMinAddress(minExtAddr);
        }
    }

    @Override
    public long getMinOffsetInQueue() {
        return this.minLogicOffset / CQ_STORE_UNIT_SIZE;
    }

    @Override
    public void putMessagePositionInfoWrapper(DispatchRequest request) {
        final int maxRetries = 30;
        boolean canWrite = this.messageStore.getRunningFlags().isCQWriteable();
        // 写入ConsumeQueue，重试最多30次
        for (int i = 0; i < maxRetries && canWrite; i++) {
            long tagsCode = request.getTagsCode();
            if (isExtWriteEnable()) {
                ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                cqExtUnit.setFilterBitMap(request.getBitMap());
                cqExtUnit.setMsgStoreTime(request.getStoreTimestamp());
                cqExtUnit.setTagsCode(request.getTagsCode());

                long extAddr = this.consumeQueueExt.put(cqExtUnit);
                if (isExtAddr(extAddr)) {
                    tagsCode = extAddr;
                } else {
                    log.warn("Save consume queue extend fail, So just save tagsCode! {}, topic:{}, queueId:{}, offset:{}", cqExtUnit,
                        topic, queueId, request.getCommitLogOffset());
                }
            }
            // 写入ConsumeQueue，注意这里还未强制刷盘
            boolean result = this.putMessagePositionInfo(request.getCommitLogOffset(),
                request.getMsgSize(), tagsCode, request.getConsumeQueueOffset());
            if (result) {
                // 如果是SLAVE，在写入成功后更新CheckPoint中的最新写入时间。是为了修复在SLAVE中ConsumeQueue异常恢复慢的问题
                // 因为在当前的设计中，没有更新SLAVE的消费队列时间戳到CheckPoint中的逻辑，所以在SLAVE中在doReput()逻辑中更新该时间戳
                // https://github.com/apache/rocketmq/pull/1455
                if (this.messageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE ||
                    this.messageStore.getMessageStoreConfig().isEnableDLegerCommitLog()) {
                    this.messageStore.getStoreCheckpoint().setPhysicMsgTimestamp(request.getStoreTimestamp());
                }
                this.messageStore.getStoreCheckpoint().setLogicsMsgTimestamp(request.getStoreTimestamp());
                if (checkMultiDispatchQueue(request)) {
                    multiDispatchLmqQueue(request, maxRetries);
                }
                return;
            } else {
                // 只有一种情况会失败，创建新的MapedFile时报错或者超时
                // 写入失败，等待1s继续写入，直到30次都失败
                // XXX: warn and notify me
                log.warn("[BUG]put commit log position info to " + topic + ":" + queueId + " " + request.getCommitLogOffset()
                    + " failed, retry " + i + " times");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn("", e);
                }
            }
        }

        // XXX: warn and notify me
        log.error("[BUG]consume queue can not write, {} {}", this.topic, this.queueId);
        this.messageStore.getRunningFlags().makeLogicsQueueError();
    }

    /**
     * 判断消息是否需要执行多队列分发
     *
     * @param dispatchRequest 投递请求
     * @return 是否需要分发
     */
    private boolean checkMultiDispatchQueue(DispatchRequest dispatchRequest) {
        if (!this.messageStore.getMessageStoreConfig().isEnableMultiDispatch()) {
            return false;
        }
        Map<String, String> prop = dispatchRequest.getPropertiesMap();
        if (prop == null || prop.isEmpty()) {
            return false;
        }
        String multiDispatchQueue = prop.get(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        String multiQueueOffset = prop.get(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET);
        if (StringUtils.isBlank(multiDispatchQueue) || StringUtils.isBlank(multiQueueOffset)) {
            return false;
        }
        return true;
    }

    /**
     * Light message queue 分发到多个队列
     *
     * @param request 分发请求
     * @param maxRetries 最大重试次数，默认 30
     */
    private void multiDispatchLmqQueue(DispatchRequest request, int maxRetries) {
        Map<String, String> prop = request.getPropertiesMap();
        String multiDispatchQueue = prop.get(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        String multiQueueOffset = prop.get(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET);
        String[] queues = multiDispatchQueue.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
        String[] queueOffsets = multiQueueOffset.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
        if (queues.length != queueOffsets.length) {
            log.error("[bug] queues.length!=queueOffsets.length ", request.getTopic());
            return;
        }
        for (int i = 0; i < queues.length; i++) {
            String queueName = queues[i];
            long queueOffset = Long.parseLong(queueOffsets[i]);
            int queueId = request.getQueueId();
            // Light message queue 在每个 broker 上只有一个 queue，queueId 为 0
            if (this.messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(queueName)) {
                queueId = 0;
            }
            doDispatchLmqQueue(request, maxRetries, queueName, queueOffset, queueId);

        }
        return;
    }

    /**
     * 分发消息到消费索引
     *
     * @param request
     * @param maxRetries
     * @param queueName
     * @param queueOffset
     * @param queueId
     */
    private void doDispatchLmqQueue(DispatchRequest request, int maxRetries, String queueName, long queueOffset,
                                    int queueId) {
        // 查找 ConsumeQueue
        ConsumeQueueInterface cq = this.messageStore.findConsumeQueue(queueName, queueId);
        boolean canWrite = this.messageStore.getRunningFlags().isCQWriteable();
        for (int i = 0; i < maxRetries && canWrite; i++) {
            // 向 ConsumeQueue 写入索引项
            boolean result = ((ConsumeQueue) cq).putMessagePositionInfo(request.getCommitLogOffset(), request.getMsgSize(),
                    request.getTagsCode(),
                    queueOffset);
            if (result) {
                break;
            } else {
                log.warn("[BUG]put commit log position info to " + queueName + ":" + queueId + " " + request.getCommitLogOffset()
                        + " failed, retry " + i + " times");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn("", e);
                }
            }
        }
    }

    /**
     * 查询要分发的队列的逻辑偏移量，放入消息属性
     *
     * @param queueOffsetAssigner the delegated queue offset assigner
     * @param msg message itself
     * @param messageNum message number
     */
    @Override
    public void assignQueueOffset(QueueOffsetAssigner queueOffsetAssigner, MessageExtBrokerInner msg,
        short messageNum) {
        String topicQueueKey = getTopic() + "-" + getQueueId();
        long queueOffset = queueOffsetAssigner.assignQueueOffset(topicQueueKey, messageNum);
        msg.setQueueOffset(queueOffset);
        // 轻量级队列分发准备，为消息添加多队列分发属性
        // For LMQ
        if (!messageStore.getMessageStoreConfig().isEnableMultiDispatch()) {
            return;
        }
        String multiDispatchQueue = msg.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        if (StringUtils.isBlank(multiDispatchQueue)) {
            return;
        }
        // 从原始消息属性中获取分发的队列列表
        String[] queues = multiDispatchQueue.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
        // 从队列偏移量表中查询当前队列偏移量
        Long[] queueOffsets = new Long[queues.length];
        for (int i = 0; i < queues.length; i++) {
            String key = queueKey(queues[i], msg);
            if (messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(key)) {
                queueOffsets[i] = queueOffsetAssigner.assignLmqOffset(key, (short) 1);
            }
        }
        // 将队列偏移量作为属性存入消息
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET,
            StringUtils.join(queueOffsets, MixAll.MULTI_DISPATCH_QUEUE_SPLITTER));
        // 移除消息的 WAIT_STORE 属性，节省存储空间
        removeWaitStorePropertyString(msg);
    }

    /**
     * 构造队列 Key
     *
     * @param queueName 轻量级队列名称
     * @param msgInner 原始消息
     * @return "队列名称-ID"
     */
    public String queueKey(String queueName, MessageExtBrokerInner msgInner) {
        StringBuilder keyBuilder = new StringBuilder();
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

    /**
     * 往ConsumeQueue中写入索引项，putMessagePositionInfo只有一个线程调用，所以不需要加锁
     *
     * @param offset CommitLog offset
     * @param size 消息在CommitLog存储的大小
     * @param tagsCode 过滤tag的hashcode
     * @param cqOffset 消息在ConsumeQueue中的逻辑偏移量。在 {@link CommitLog#doAppend} 方法中已经生成并保存
     * @return 是否成功
     */
    private boolean putMessagePositionInfo(final long offset, final int size, final long tagsCode,
        final long cqOffset) {

        // CommitLog offset + size 小于ConsumeQueue中保存的最大CommitLog物理偏移量，说明这个消息重复生成ConsumeQueue，直接返回
        // 多见于关机恢复的场景。关机恢复从倒数第3个CommitLog文件开始重新转发消息生成ConsumeQueue
        if (offset + size <= this.maxPhysicOffset) {
            log.warn("Maybe try to build consume queue repeatedly maxPhysicOffset={} phyOffset={}", maxPhysicOffset, offset);
            return true;
        }

        // NIO ByteBuffer 写入三个参数
        this.byteBufferIndex.flip();
        this.byteBufferIndex.limit(CQ_STORE_UNIT_SIZE);
        this.byteBufferIndex.putLong(offset);
        this.byteBufferIndex.putInt(size);
        this.byteBufferIndex.putLong(tagsCode);

        // 计算本次期望写入ConsumeQueue的物理偏移量
        final long expectLogicOffset = cqOffset * CQ_STORE_UNIT_SIZE;

        // 根据期望的偏移量找到对应的内存映射文件
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(expectLogicOffset);
        if (mappedFile != null) {
            // 纠正MappedFile逻辑队列索引顺序
            // 如果MappedFileQueue中的MappedFile列表被删除
            // 这时需要保证消息队列的逻辑位置和ConsumeQueue文件的起始文件的偏移量一致，要补充空的消息索引
            if (mappedFile.isFirstCreateInQueue() && cqOffset != 0 && mappedFile.getWrotePosition() == 0) {
                this.minLogicOffset = expectLogicOffset;
                this.mappedFileQueue.setFlushedWhere(expectLogicOffset);
                this.mappedFileQueue.setCommittedWhere(expectLogicOffset);
                // 填充空的消息索引
                this.fillPreBlank(mappedFile, expectLogicOffset);
                log.info("fill pre blank space " + mappedFile.getFileName() + " " + expectLogicOffset + " "
                    + mappedFile.getWrotePosition());
            }

            if (cqOffset != 0) {
                // 当前ConsumeQueue被写过的物理offset = 该MappedFile被写过的位置 + 该MappedFile起始物理偏移量
                // 注意：此时消息还没从内存刷到磁盘，如果是异步刷盘，Broker断电就会存在数据丢失的情况
                // 此时消费者消费不到，所以在重要业务中使用同步刷盘确保数据不丢失
                long currentLogicOffset = mappedFile.getWrotePosition() + mappedFile.getFileFromOffset();

                // 如果期望写入的位置 < 当前ConsumeQueue被写过的位置，说明是重复写入，直接返回
                if (expectLogicOffset < currentLogicOffset) {
                    log.warn("Build  consume queue repeatedly, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                        expectLogicOffset, currentLogicOffset, this.topic, this.queueId, expectLogicOffset - currentLogicOffset);
                    return true;
                }

                // 期望写入的位置应该等于被写过的位置
                if (expectLogicOffset != currentLogicOffset) {
                    LOG_ERROR.warn(
                        "[BUG]logic queue order maybe wrong, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                        expectLogicOffset,
                        currentLogicOffset,
                        this.topic,
                        this.queueId,
                        expectLogicOffset - currentLogicOffset
                    );
                }
            }
            this.maxPhysicOffset = offset + size;
            // 将一个ConsumeQueue数据写盘，此时并未刷盘
            return mappedFile.appendMessage(this.byteBufferIndex.array());
        }
        return false;
    }

    private void fillPreBlank(final MappedFile mappedFile, final long untilWhere) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
        byteBuffer.putLong(0L);
        byteBuffer.putInt(Integer.MAX_VALUE);
        byteBuffer.putLong(0L);

        int until = (int) (untilWhere % this.mappedFileQueue.getMappedFileSize());
        for (int i = 0; i < until; i += CQ_STORE_UNIT_SIZE) {
            mappedFile.appendMessage(byteBuffer.array());
        }
    }

    /**
     * 返回Index Buffer
     *
     * @param startIndex 起始偏移量索引
     */
    public SelectMappedBufferResult getIndexBuffer(final long startIndex) {
        int mappedFileSize = this.mappedFileSize;
        long offset = startIndex * CQ_STORE_UNIT_SIZE;
        if (offset >= this.getMinLogicOffset()) {
            MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
            if (mappedFile != null) {
                return mappedFile.selectMappedBuffer((int) (offset % mappedFileSize));
            }
        }
        return null;
    }

    @Override
    public ReferredIterator<CqUnit> iterateFrom(long startOffset) {
        SelectMappedBufferResult sbr = getIndexBuffer(startOffset);
        if (sbr == null) {
            return null;
        }
        return new ConsumeQueueIterator(sbr);
    }

    @Override
    public CqUnit get(long offset) {
        ReferredIterator<CqUnit> it = iterateFrom(offset);
        if (it == null) {
            return null;
        }
        return it.nextAndRelease();
    }

    @Override
    public CqUnit getEarliestUnit() {
        /**
         * here maybe should not return null
         */
        ReferredIterator<CqUnit> it = iterateFrom(minLogicOffset / CQ_STORE_UNIT_SIZE);
        if (it == null) {
            return null;
        }
        return it.nextAndRelease();
    }

    @Override
    public CqUnit getLatestUnit() {
        ReferredIterator<CqUnit> it = iterateFrom((mappedFileQueue.getMaxOffset() / CQ_STORE_UNIT_SIZE) - 1);
        if (it == null) {
            return null;
        }
        return it.nextAndRelease();
    }

    @Override
    public boolean isFirstFileAvailable() {
        return false;
    }

    @Override
    public boolean isFirstFileExist() {
        return false;
    }

    private class ConsumeQueueIterator implements ReferredIterator<CqUnit> {
        private SelectMappedBufferResult sbr;
        private int relativePos = 0;

        public ConsumeQueueIterator(SelectMappedBufferResult sbr) {
            this.sbr =  sbr;
            if (sbr != null && sbr.getByteBuffer() != null) {
                relativePos = sbr.getByteBuffer().position();
            }
        }

        @Override
        public boolean hasNext() {
            if (sbr == null || sbr.getByteBuffer() == null) {
                return false;
            }

            return sbr.getByteBuffer().hasRemaining();
        }

        @Override
        public CqUnit next() {
            if (!hasNext()) {
                return null;
            }
            long queueOffset = (sbr.getStartOffset() + sbr.getByteBuffer().position() -  relativePos) / CQ_STORE_UNIT_SIZE;
            CqUnit cqUnit = new CqUnit(queueOffset,
                    sbr.getByteBuffer().getLong(),
                    sbr.getByteBuffer().getInt(),
                    sbr.getByteBuffer().getLong());

            if (isExtAddr(cqUnit.getTagsCode())) {
                ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                boolean extRet = getExt(cqUnit.getTagsCode(), cqExtUnit);
                if (extRet) {
                    cqUnit.setTagsCode(cqExtUnit.getTagsCode());
                    cqUnit.setCqExtUnit(cqExtUnit);
                } else {
                    // can't find ext content.Client will filter messages by tag also.
                    log.error("[BUG] can't find consume queue extend file content! addr={}, offsetPy={}, sizePy={}, topic={}",
                            cqUnit.getTagsCode(), cqUnit.getPos(), cqUnit.getPos(), getTopic());
                }
            }
            return cqUnit;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove");
        }

        @Override
        public void release() {
            if (sbr != null) {
                sbr.release();
                sbr = null;
            }
        }

        @Override
        public CqUnit nextAndRelease() {
            try {
                return next();
            } finally {
                release();
            }
        }
    }

    public ConsumeQueueExt.CqExtUnit getExt(final long offset) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset);
        }
        return null;
    }

    public boolean getExt(final long offset, ConsumeQueueExt.CqExtUnit cqExtUnit) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset, cqExtUnit);
        }
        return false;
    }

    @Override
    public long getMinLogicOffset() {
        return minLogicOffset;
    }

    public void setMinLogicOffset(long minLogicOffset) {
        this.minLogicOffset = minLogicOffset;
    }

    @Override
    public long rollNextFile(final long nextBeginOffset) {
        int mappedFileSize = this.mappedFileSize;
        int totalUnitsInFile = mappedFileSize / CQ_STORE_UNIT_SIZE;
        return nextBeginOffset + totalUnitsInFile - nextBeginOffset % totalUnitsInFile;
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public int getQueueId() {
        return queueId;
    }

    @Override
    public CQType getCQType() {
        return CQType.SimpleCQ;
    }

    @Override
    public long getMaxPhysicOffset() {
        return maxPhysicOffset;
    }

    public void setMaxPhysicOffset(long maxPhysicOffset) {
        this.maxPhysicOffset = maxPhysicOffset;
    }

    @Override
    public void destroy() {
        this.maxPhysicOffset = -1;
        this.minLogicOffset = 0;
        this.mappedFileQueue.destroy();
        if (isExtReadEnable()) {
            this.consumeQueueExt.destroy();
        }
    }

    /**
     * 获取当前队列中的消息总数
     */
    @Override
    public long getMessageTotalInQueue() {
        return this.getMaxOffsetInQueue() - this.getMinOffsetInQueue();
    }

    @Override
    public long getMaxOffsetInQueue() {
        return this.mappedFileQueue.getMaxOffset() / CQ_STORE_UNIT_SIZE;
    }

    @Override
    public void checkSelf() {
        mappedFileQueue.checkSelf();
        if (isExtReadEnable()) {
            this.consumeQueueExt.checkSelf();
        }
    }

    protected boolean isExtReadEnable() {
        return this.consumeQueueExt != null;
    }

    protected boolean isExtWriteEnable() {
        return this.consumeQueueExt != null
            && this.messageStore.getMessageStoreConfig().isEnableConsumeQueueExt();
    }

    /**
     * Check {@code tagsCode} is address of extend file or tags code.
     */
    public boolean isExtAddr(long tagsCode) {
        return ConsumeQueueExt.isExtAddr(tagsCode);
    }

    @Override
    public void swapMap(int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs) {
        mappedFileQueue.swapMap(reserveNum, forceSwapIntervalMs, normalSwapIntervalMs);
    }

    @Override
    public void cleanSwappedMap(long forceCleanSwapIntervalMs) {
        mappedFileQueue.cleanSwappedMap(forceCleanSwapIntervalMs);
    }

    @Override
    public long estimateMessageCount(long from, long to, MessageFilter filter) {
        long physicalOffsetFrom = from * CQ_STORE_UNIT_SIZE;
        long physicalOffsetTo = to * CQ_STORE_UNIT_SIZE;
        List<MappedFile> mappedFiles = mappedFileQueue.range(physicalOffsetFrom, physicalOffsetTo);
        if (mappedFiles.isEmpty()) {
            return -1;
        }

        boolean sample = false;
        long match = 0;
        long raw = 0;

        for (MappedFile mappedFile : mappedFiles) {
            int start = 0;
            int len = mappedFile.getFileSize();

            // calculate start and len for first segment and last segment to reduce scanning
            // first file segment
            if (mappedFile.getFileFromOffset() <= physicalOffsetFrom) {
                start = (int) (physicalOffsetFrom - mappedFile.getFileFromOffset());
                if (mappedFile.getFileFromOffset() + mappedFile.getFileSize() >= physicalOffsetTo) {
                    // current mapped file covers search range completely.
                    len = (int) (physicalOffsetTo - physicalOffsetFrom);
                } else {
                    len = mappedFile.getFileSize() - start;
                }
            }

            // last file segment
            if (0 == start && mappedFile.getFileFromOffset() + mappedFile.getFileSize() > physicalOffsetTo) {
                len = (int) (physicalOffsetTo - mappedFile.getFileFromOffset());
            }

            // select partial data to scan
            SelectMappedBufferResult slice = mappedFile.selectMappedBuffer(start, len);
            if (null != slice) {
                try {
                    ByteBuffer buffer = slice.getByteBuffer();
                    int current = 0;
                    while (current < len) {
                        // skip physicalOffset and message length fields.
                        buffer.position(current + MSG_TAG_OFFSET_INDEX);
                        long tagCode = buffer.getLong();
                        ConsumeQueueExt.CqExtUnit ext = null;
                        if (isExtWriteEnable()) {
                            ext = consumeQueueExt.get(tagCode);
                            tagCode = ext.getTagsCode();
                        }
                        if (filter.isMatchedByConsumeQueue(tagCode, ext)) {
                            match++;
                        }
                        raw++;
                        current += CQ_STORE_UNIT_SIZE;

                        if (raw >= messageStore.getMessageStoreConfig().getMaxConsumeQueueScan()) {
                            sample = true;
                            break;
                        }

                        if (match > messageStore.getMessageStoreConfig().getSampleCountThreshold()) {
                            sample = true;
                            break;
                        }
                    }
                } finally {
                    slice.release();
                }
            }
            // we have scanned enough entries, now is the time to return an educated guess.
            if (sample) {
                break;
            }
        }

        long result = match;
        if (sample) {
            if (0 == raw) {
                log.error("[BUG]. Raw should NOT be 0");
                return 0;
            }
            result = (long) (match * (to - from) * 1.0 / raw);
        }
        log.debug("Result={}, raw={}, match={}, sample={}", result, raw, match, sample);
        return result;
    }
}
