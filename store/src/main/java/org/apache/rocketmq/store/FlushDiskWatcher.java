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


import java.util.concurrent.LinkedBlockingQueue;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.CommitLog.GroupCommitRequest;

/**
 * 同步刷盘请求超时监控服务
 */
public class FlushDiskWatcher extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final LinkedBlockingQueue<GroupCommitRequest> commitRequests = new LinkedBlockingQueue<>();

    @Override
    public String getServiceName() {
        return FlushDiskWatcher.class.getSimpleName();
    }

    @Override
    public void run() {
        while (!isStopped()) {
            GroupCommitRequest request = null;
            try {
                request = commitRequests.take();
            } catch (InterruptedException e) {
                log.warn("take flush disk commit request, but interrupted, this may caused by shutdown");
                continue;
            }
            // 检查刷盘请求是否完成或超时，如果超时则设置刷盘请求状态为超时，结束刷盘请求 future，如果未完成则等到超时时间到达再检查
            while (!request.future().isDone()) {
                long now = System.nanoTime();
                if (now - request.getDeadLine() >= 0) {
                    // 如果已经超时，设置刷盘请求状态为超时，结束刷盘请求 future
                    request.wakeupCustomer(PutMessageStatus.FLUSH_DISK_TIMEOUT);
                    break;
                }
                // To avoid frequent thread switching, replace future.get with sleep here,
                long sleepTime = (request.getDeadLine() - now) / 1_000_000;
                sleepTime = Math.min(10, sleepTime);
                if (sleepTime == 0) {
                    request.wakeupCustomer(PutMessageStatus.FLUSH_DISK_TIMEOUT);
                    break;
                }
                // 等待，直到刷盘请求超时时间到达，然后再检查刷盘请求是否完成
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    log.warn(
                            "An exception occurred while waiting for flushing disk to complete. this may caused by shutdown");
                    break;
                }
            }
        }
    }

    public void add(GroupCommitRequest request) {
        commitRequests.add(request);
    }

    public int queueSize() {
        return commitRequests.size();
    }
}
