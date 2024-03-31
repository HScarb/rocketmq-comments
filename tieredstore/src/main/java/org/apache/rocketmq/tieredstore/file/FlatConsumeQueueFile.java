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
package org.apache.rocketmq.tieredstore.file;

import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.provider.FileSegmentFactory;

/**
 * 指向 {@link FlatCommitLogFile} 偏移量的一个索引，严格连续递增
 * 实际索引的位置会从指向 {@link org.apache.rocketmq.store.CommitLog} 的位置改为 {@link FlatCommitLogFile} 的偏移量
 */
public class FlatConsumeQueueFile extends FlatAppendFile {

    public FlatConsumeQueueFile(FileSegmentFactory fileSegmentFactory, String filePath) {
        super(fileSegmentFactory, FileSegmentType.CONSUME_QUEUE, filePath);
    }
}
