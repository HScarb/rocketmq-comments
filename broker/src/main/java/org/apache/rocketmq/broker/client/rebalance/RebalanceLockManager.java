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
package org.apache.rocketmq.broker.client.rebalance;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 负载管理
 * 顺序消费使用
 */
public class RebalanceLockManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.REBALANCE_LOCK_LOGGER_NAME);
    private final static long REBALANCE_LOCK_MAX_LIVE_TIME = Long.parseLong(System.getProperty(
        "rocketmq.broker.rebalance.lockMaxLiveTime", "60000"));
    // 锁容器读写锁
    private final Lock lock = new ReentrantLock();
    // 消息队列锁定状态表
    private final ConcurrentMap<String/* group */, ConcurrentHashMap<MessageQueue, LockEntry>> mqLockTable =
        new ConcurrentHashMap<>(1024);

    public boolean isLockAllExpired(final String group) {
        final ConcurrentHashMap<MessageQueue, LockEntry> lockEntryMap = mqLockTable.get(group);
        if (null == lockEntryMap) {
            return true;
        }
        for (LockEntry entry : lockEntryMap.values()) {
            if (!entry.isExpired()) {
                return false;
            }
        }
        return true;
    }

    public boolean tryLock(final String group, final MessageQueue mq, final String clientId) {

        if (!this.isLocked(group, mq, clientId)) {
            try {
                this.lock.lockInterruptibly();
                try {
                    ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                    if (null == groupValue) {
                        groupValue = new ConcurrentHashMap<>(32);
                        this.mqLockTable.put(group, groupValue);
                    }

                    LockEntry lockEntry = groupValue.get(mq);
                    if (null == lockEntry) {
                        lockEntry = new LockEntry();
                        lockEntry.setClientId(clientId);
                        groupValue.put(mq, lockEntry);
                        log.info(
                            "RebalanceLockManager#tryLock: lock a message queue which has not been locked yet, "
                                + "group={}, clientId={}, mq={}", group, clientId, mq);
                    }

                    if (lockEntry.isLocked(clientId)) {
                        lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                        return true;
                    }

                    String oldClientId = lockEntry.getClientId();

                    if (lockEntry.isExpired()) {
                        lockEntry.setClientId(clientId);
                        lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                        log.warn(
                            "RebalanceLockManager#tryLock: try to lock a expired message queue, group={}, mq={}, old "
                                + "client id={}, new client id={}", group, mq, oldClientId, clientId);
                        return true;
                    }

                    log.warn(
                        "RebalanceLockManager#tryLock: message queue has been locked by other client, group={}, "
                            + "mq={}, locked client id={}, current client id={}", group, mq, oldClientId, clientId);
                    return false;
                } finally {
                    this.lock.unlock();
                }
            } catch (InterruptedException e) {
                log.error("RebalanceLockManager#tryLock: unexpected error, group={}, mq={}, clientId={}", group, mq,
                    clientId, e);
            }
        } else {

        }

        return true;
    }

    private boolean isLocked(final String group, final MessageQueue mq, final String clientId) {
        ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
        if (groupValue != null) {
            LockEntry lockEntry = groupValue.get(mq);
            if (lockEntry != null) {
                boolean locked = lockEntry.isLocked(clientId);
                if (locked) {
                    lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                }

                return locked;
            }
        }

        return false;
    }

    /**
     * 批量锁定消息队列
     *
     * @param group 消费组
     * @param mqs 要锁定的消息队列
     * @param clientId 客户端 ID
     * @return 锁定成功的消息队列
     */
    public Set<MessageQueue> tryLockBatch(final String group, final Set<MessageQueue> mqs,
        final String clientId) {
        // 要锁定的队列中已经锁定的队列
        Set<MessageQueue> lockedMqs = new HashSet<>(mqs.size());
        // 之前没有锁定，需要锁定的队列
        Set<MessageQueue> notLockedMqs = new HashSet<>(mqs.size());

        for (MessageQueue mq : mqs) {
            if (this.isLocked(group, mq, clientId)) {
                lockedMqs.add(mq);
            } else {
                notLockedMqs.add(mq);
            }
        }

        // 锁定需要锁定的队列
        if (!notLockedMqs.isEmpty()) {
            try {
                this.lock.lockInterruptibly();
                try {
                    ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                    if (null == groupValue) {
                        groupValue = new ConcurrentHashMap<>(32);
                        this.mqLockTable.put(group, groupValue);
                    }

                    for (MessageQueue mq : notLockedMqs) {
                        // 为队列新建锁定标识，加入锁定状态表
                        LockEntry lockEntry = groupValue.get(mq);
                        if (null == lockEntry) {
                            lockEntry = new LockEntry();
                            lockEntry.setClientId(clientId);
                            groupValue.put(mq, lockEntry);
                            log.info(
                                "RebalanceLockManager#tryLockBatch: lock a message which has not been locked yet, "
                                    + "group={}, clientId={}, mq={}", group, clientId, mq);
                        }

                        if (lockEntry.isLocked(clientId)) {
                            lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                            lockedMqs.add(mq);
                            continue;
                        }

                        String oldClientId = lockEntry.getClientId();

                        if (lockEntry.isExpired()) {
                            lockEntry.setClientId(clientId);
                            lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                            log.warn(
                                "RebalanceLockManager#tryLockBatch: try to lock a expired message queue, group={}, "
                                    + "mq={}, old client id={}, new client id={}", group, mq, oldClientId, clientId);
                            lockedMqs.add(mq);
                            continue;
                        }

                        log.warn(
                            "RebalanceLockManager#tryLockBatch: message queue has been locked by other client, "
                                + "group={}, mq={}, locked client id={}, current client id={}", group, mq, oldClientId,
                            clientId);
                    }
                } finally {
                    this.lock.unlock();
                }
            } catch (InterruptedException e) {
                log.error("RebalanceLockManager#tryBatch: unexpected error, group={}, mqs={}, clientId={}", group, mqs,
                    clientId, e);
            }
        }

        return lockedMqs;
    }

    public void unlockBatch(final String group, final Set<MessageQueue> mqs, final String clientId) {
        try {
            this.lock.lockInterruptibly();
            try {
                ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                if (null != groupValue) {
                    for (MessageQueue mq : mqs) {
                        LockEntry lockEntry = groupValue.get(mq);
                        if (null != lockEntry) {
                            if (lockEntry.getClientId().equals(clientId)) {
                                groupValue.remove(mq);
                                log.info("RebalanceLockManager#unlockBatch: unlock mq, group={}, clientId={}, mqs={}",
                                    group, clientId, mq);
                            } else {
                                log.warn(
                                    "RebalanceLockManager#unlockBatch: mq locked by other client, group={}, locked "
                                        + "clientId={}, current clientId={}, mqs={}", group, lockEntry.getClientId(),
                                    clientId, mq);
                            }
                        } else {
                            log.warn("RebalanceLockManager#unlockBatch: mq not locked, group={}, clientId={}, mq={}",
                                group, clientId, mq);
                        }
                    }
                } else {
                    log.warn("RebalanceLockManager#unlockBatch: group not exist, group={}, clientId={}, mqs={}", group,
                        clientId, mqs);
                }
            } finally {
                this.lock.unlock();
            }
        } catch (InterruptedException e) {
            log.error("RebalanceLockManager#unlockBatch: unexpected error, group={}, mqs={}, clientId={}", group, mqs,
                clientId);
        }
    }

    /**
     * 锁定标识，承载表示客户端 ID 和锁定时间
     */
    static class LockEntry {
        // 客户端 ID
        private String clientId;
        private volatile long lastUpdateTimestamp = System.currentTimeMillis();

        public String getClientId() {
            return clientId;
        }

        public void setClientId(String clientId) {
            this.clientId = clientId;
        }

        public long getLastUpdateTimestamp() {
            return lastUpdateTimestamp;
        }

        public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
            this.lastUpdateTimestamp = lastUpdateTimestamp;
        }

        /**
         * 是否锁定
         *
         * @param clientId
         * @return
         */
        public boolean isLocked(final String clientId) {
            boolean eq = this.clientId.equals(clientId);
            return eq && !this.isExpired();
        }

        /**
         * 锁是否过期，过期时间为 1 分钟
         *
         * @return
         */
        public boolean isExpired() {
            boolean expired =
                (System.currentTimeMillis() - this.lastUpdateTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;

            return expired;
        }
    }
}
