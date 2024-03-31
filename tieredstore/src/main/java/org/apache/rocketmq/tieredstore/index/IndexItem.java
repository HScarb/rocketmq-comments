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

import java.nio.ByteBuffer;

/**
 * 分级存储文件索引项，在本地 {@link org.apache.rocketmq.store.index.IndexFile} 的基础上多了 topicId、queueId、size
 * 适配 非压缩/压缩 这两种形态
 */
public class IndexItem {

    /**
     * 压缩前的索引项大小
     */
    public static final int INDEX_ITEM_SIZE = 32;
    /**
     * 压缩后的索引项大小
     */
    public static final int COMPACT_INDEX_ITEM_SIZE = 28;

    /**
     * key 哈希
     */
    private final int hashCode;
    /**
     * 新增，分级存储 topic ID。分级存储中，broker 为每个 topic 分配一个唯一 ID，顺序递增，存储在元数据文件中，topicMetadataTable
     */
    private final int topicId;
    /**
     * 新增，分级存储 queue ID，queueMetadataTable
     */
    private final int queueId;
    /**
     * 消息物理 offset
     */
    private final long offset;
    /**
     * 新增，消息数据长度
     */
    private final int size;
    /**
     * 消息保存时间与索引文件最早消息保存时间的差值，用于搜索时间范围内的消息
     */
    private final int timeDiff;
    /**
     * 指向下一个索引项位置的指针，压缩格式下不存在
     */
    private final int itemIndex;

    public IndexItem(int topicId, int queueId, long offset, int size, int hashCode, int timeDiff, int itemIndex) {
        this.hashCode = hashCode;
        this.topicId = topicId;
        this.queueId = queueId;
        this.offset = offset;
        this.size = size;
        this.timeDiff = timeDiff;
        this.itemIndex = itemIndex;
    }

    public IndexItem(byte[] bytes) {
        if (bytes == null ||
            bytes.length != INDEX_ITEM_SIZE &&
                bytes.length != COMPACT_INDEX_ITEM_SIZE) {
            throw new IllegalArgumentException("Byte array length not correct");
        }

        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        hashCode = byteBuffer.getInt(0);
        topicId = byteBuffer.getInt(4);
        queueId = byteBuffer.getInt(8);
        offset = byteBuffer.getLong(12);
        size = byteBuffer.getInt(20);
        timeDiff = byteBuffer.getInt(24);
        itemIndex = bytes.length == INDEX_ITEM_SIZE ? byteBuffer.getInt(28) : 0;
    }

    public ByteBuffer getByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(32);
        byteBuffer.putInt(0, hashCode);
        byteBuffer.putInt(4, topicId);
        byteBuffer.putInt(8, queueId);
        byteBuffer.putLong(12, offset);
        byteBuffer.putInt(20, size);
        byteBuffer.putInt(24, timeDiff);
        byteBuffer.putInt(28, itemIndex);
        return byteBuffer;
    }

    public int getHashCode() {
        return hashCode;
    }

    public int getTopicId() {
        return topicId;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getOffset() {
        return offset;
    }

    public int getSize() {
        return size;
    }

    public int getTimeDiff() {
        return timeDiff;
    }

    public int getItemIndex() {
        return itemIndex;
    }

    @Override
    public String toString() {
        return "IndexItem{" +
            "hashCode=" + hashCode +
            ", topicId=" + topicId +
            ", queueId=" + queueId +
            ", offset=" + offset +
            ", size=" + size +
            ", timeDiff=" + timeDiff +
            ", position=" + itemIndex +
            '}';
    }
}
