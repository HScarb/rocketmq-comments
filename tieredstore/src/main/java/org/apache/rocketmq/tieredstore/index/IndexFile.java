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

public interface IndexFile extends IndexService {

    /**
     * Enumeration for the status of the index file.
     * <ul>
     *     <li>{@link #UNSEALED}: 初始状态，未被压缩，正被写入。类似本地 {@link org.apache.rocketmq.store.index.IndexFile} 的格式。路径为 {storePath}/tiered_index_file/{时间戳}</li>
     *     <li>{@link #SEALED}: 已经或正在被压缩成新的 IndexFile，还未上传到二级存储。路径为{storePath}/tiered_index_file/compacting/{时间戳}</li>
     *     <li>{@link #UPLOAD}: 已经上传到二级存储</li>
     * </ul>
     */
    enum IndexStatusEnum {
        SHUTDOWN, UNSEALED, SEALED, UPLOAD
    }

    long getTimestamp();

    long getEndTimestamp();

    /**
     * 获取索引文件当前的状态
     * @return
     */
    IndexStatusEnum getFileStatus();

    ByteBuffer doCompaction();
}
