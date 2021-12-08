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
package org.apache.rocketmq.store.ha;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 用来做线程之间异步通知
 */
public class WaitNotifyObject {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // 是否已经被Notify过，广播模式
    protected final ConcurrentHashMap<Long/* thread id */, AtomicBoolean/* notified */> waitingThreadTable =
        new ConcurrentHashMap<Long, AtomicBoolean>(16);
    // 是否已经被Notify过
    protected AtomicBoolean hasNotified = new AtomicBoolean(false);

    public void wakeup() {
        boolean needNotify = hasNotified.compareAndSet(false, true);
        if (needNotify) {
            synchronized (this) {
                this.notify();
            }
        }
    }

    protected void waitForRunning(long interval) {
        if (this.hasNotified.compareAndSet(true, false)) {
            this.onWaitEnd();
            return;
        }
        synchronized (this) {
            try {
                if (this.hasNotified.compareAndSet(true, false)) {
                    this.onWaitEnd();
                    return;
                }
                this.wait(interval);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            } finally {
                this.hasNotified.set(false);
                this.onWaitEnd();
            }
        }
    }

    protected void onWaitEnd() {
    }

    /**
     * 广播方式唤醒
     */
    public void wakeupAll() {
        boolean needNotify = false;
        for (Map.Entry<Long,AtomicBoolean> entry : this.waitingThreadTable.entrySet()) {
            if (entry.getValue().compareAndSet(false, true)) {
                needNotify = true;
            }
        }
        if (needNotify) {
            synchronized (this) {
                this.notifyAll();
            }
        }
    }
    
    /**
     * 多个线程调用wait
     */
    public void allWaitForRunning(long interval) {
        long currentThreadId = Thread.currentThread().getId();
        AtomicBoolean notified = this.waitingThreadTable.computeIfAbsent(currentThreadId, k -> new AtomicBoolean(false));
        if (notified.compareAndSet(true, false)) {
            this.onWaitEnd();
            return;
        }
        synchronized (this) {
            try {
                if (notified.compareAndSet(true, false)) {
                    this.onWaitEnd();
                    return;
                }
                this.wait(interval);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            } finally {
                notified.set(false);
                this.onWaitEnd();
            }
        }
    }

    public void removeFromWaitingThreadTable() {
        long currentThreadId = Thread.currentThread().getId();
        synchronized (this) {
            this.waitingThreadTable.remove(currentThreadId);
        }
    }
}
