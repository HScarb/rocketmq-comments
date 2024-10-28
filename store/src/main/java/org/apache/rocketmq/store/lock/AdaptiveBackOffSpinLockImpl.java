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
package org.apache.rocketmq.store.lock;

import org.apache.rocketmq.store.config.MessageStoreConfig;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 自适应锁，根据当前的 TPS 和线程数，动态调整自旋锁和可重入锁之间的切换
 */
public class AdaptiveBackOffSpinLockImpl implements AdaptiveBackOffSpinLock {
    /**
     * 当前使用的锁类型，初始为 {@link BackOffSpinLock}
     */
    private AdaptiveBackOffSpinLock adaptiveLock;

    /**
     * 锁是否可用，true 为锁可用，false 为正在进行锁的自适应切换，锁不可用
     */
    //state
    private AtomicBoolean state = new AtomicBoolean(true);

    // Used to determine the switchover between a mutex lock and a spin lock
    private final static float SWAP_SPIN_LOCK_RATIO = 0.8f;

    /**
     * 用于调整自旋锁自旋次数 K 的比例
     */
    // It is used to adjust the spin number K of the escape spin lock
    // When (retreat number / TPS) <= (1 / BASE_SWAP_ADAPTIVE_RATIO * SPIN_LOCK_ADAPTIVE_RATIO), K is decreased
    private final static int SPIN_LOCK_ADAPTIVE_RATIO = 4;

    /**
     * 用于判断是否需要增加自旋次数 K 的比例
     */
    // It is used to adjust the spin number K of the escape spin lock
    // When (retreat number / TPS) >= (1 / BASE_SWAP_ADAPTIVE_RATIO), K is increased
    private final static int BASE_SWAP_LOCK_RATIO = 320;

    private final static String BACK_OFF_SPIN_LOCK = "SpinLock";

    private final static String REENTRANT_LOCK = "ReentrantLock";

    private Map<String, AdaptiveBackOffSpinLock> locks;

    /**
     * TPS 表，用于统计每秒的操作次数，采用双槽机制
     * 双槽统计机制将 TPS 表分为 2 个槽位，分别对应奇数和偶数秒。这样可以实现读写分离，避免多线程读写冲突。
     * 即更新 TPS 时更新当前秒的槽位，读取 TPS 时读取上一秒的槽位。
     */
    private final List<AtomicInteger> tpsTable;

    /**
     * 线程表，用于统计每秒的线程数量，采用双槽机制
     */
    private final List<Map<Thread, Byte>> threadTable;

    /**
     * 锁类型切换的临界值
     */
    private int swapCriticalPoint;

    /**
     * 当前持有锁的线程数
     */
    private AtomicInteger currentThreadNum = new AtomicInteger(0);

    /**
     * 是否开启自适应切换功能，false 则无法切换
     */
    private AtomicBoolean isOpen = new AtomicBoolean(true);

    public AdaptiveBackOffSpinLockImpl() {
        this.locks = new HashMap<>();
        this.locks.put(REENTRANT_LOCK, new BackOffReentrantLock());
        this.locks.put(BACK_OFF_SPIN_LOCK, new BackOffSpinLock());

        this.threadTable = new ArrayList<>(2);
        this.threadTable.add(new ConcurrentHashMap<>());
        this.threadTable.add(new ConcurrentHashMap<>());

        this.tpsTable = new ArrayList<>(2);
        this.tpsTable.add(new AtomicInteger(0));
        this.tpsTable.add(new AtomicInteger(0));

        adaptiveLock = this.locks.get(BACK_OFF_SPIN_LOCK);
    }

    @Override
    public void lock() {
        // 更新当前秒数槽位的 TPS 和持有锁线程数
        int slot = LocalTime.now().getSecond() % 2;
        this.threadTable.get(slot).putIfAbsent(Thread.currentThread(), Byte.MAX_VALUE);
        this.tpsTable.get(slot).getAndIncrement();
        // 确保锁可用（未进行自适应切换）
        boolean state;
        do {
            state = this.state.get();
        } while (!state);

        // 加锁
        currentThreadNum.incrementAndGet();
        this.adaptiveLock.lock();
    }

    @Override
    public void unlock() {
        // 解锁
        this.adaptiveLock.unlock();
        currentThreadNum.decrementAndGet();
        // 进行锁自适应切换
        if (isOpen.get()) {
            swap();
        }
    }

    @Override
    public void update(MessageStoreConfig messageStoreConfig) {
        this.adaptiveLock.update(messageStoreConfig);
    }

    /**
     * 根据 TPS 和线程数确定是否在自旋锁和可重入锁之间切换。</br>
     * 计算是否需要切换，并在必要时执行切换
     */
    @Override
    public void swap() {
        if (!this.state.get()) {
            return;
        }
        boolean needSwap = false;
        // 读取上一秒槽位的 TPS 和线程数
        int slot = 1 - LocalTime.now().getSecond() % 2;
        int tps = this.tpsTable.get(slot).get() + 1;
        int threadNum = this.threadTable.get(slot).size();
        // 清空上一秒槽位的数据
        this.tpsTable.get(slot).set(-1);
        this.threadTable.get(slot).clear();
        if (tps == 0) {
            return;
        }

        if (this.adaptiveLock instanceof BackOffSpinLock) {
            // 当前使用的是自旋锁
            BackOffSpinLock lock = (BackOffSpinLock) this.adaptiveLock;
            // 根据退避次数和当前 TPS，判断自旋锁繁忙情况。如果繁忙，先增加最大自旋次数，如果已经加满，则切换为可重入锁。
            // Avoid frequent adjustment of K, and make a reasonable range through experiments
            // reasonable range : (retreat number / TPS) > (1 / BASE_SWAP_ADAPTIVE_RATIO * SPIN_LOCK_ADAPTIVE_RATIO) &&
            // (retreat number / TPS) < (1 / BASE_SWAP_ADAPTIVE_RATIO)
            if (lock.getNumberOfRetreat(slot) * BASE_SWAP_LOCK_RATIO >= tps) {
                // 退避次数过多，（退避次数 * 常量 >= TPS），表示锁竞争激烈，自旋锁性能下降
                if (lock.isAdapt()) {
                    // 如果自旋锁的最大自旋次数还未达到上限，则增加它的最大自旋次数
                    lock.adapt(true);
                } else {
                    // 自旋锁的最大自旋次数已经达到上限，无法再增加自旋次数，准备切换成可重入锁
                    // It is used to switch between mutex lock and spin lock
                    this.swapCriticalPoint = tps * threadNum;
                    needSwap = true;
                }
            } else if (lock.getNumberOfRetreat(slot) * BASE_SWAP_LOCK_RATIO * SPIN_LOCK_ADAPTIVE_RATIO <= tps) {
                // 当前退避次数较少。相对于 TPS 来说，自旋次数可能过大，需要减少自旋次数
                lock.adapt(false);
            }
            // 重置退避次数为 0
            lock.setNumberOfRetreat(slot, 0);
        } else {
            // 当前使用的是可重入锁
            if (tps * threadNum <= this.swapCriticalPoint * SWAP_SPIN_LOCK_RATIO) {
                // TPS * 线程数小于临界值的 80%，表示锁竞争不激烈，切换成自旋锁，获得更高的性能
                needSwap = true;
            }
        }

        if (needSwap) {
            // 进入锁切换临界区，state 设为 false，避免多个线程进入
            if (this.state.compareAndSet(true, false)) {
                // 等待当前获取锁的线程数变为 0
                // Ensures that no threads are in contention locks as well as in critical zones
                int currentThreadNum;
                do {
                    currentThreadNum = this.currentThreadNum.get();
                } while (currentThreadNum != 0);

                // 切换锁类型
                try {
                    if (this.adaptiveLock instanceof BackOffSpinLock) {
                        this.adaptiveLock = this.locks.get(REENTRANT_LOCK);
                    } else {
                        this.adaptiveLock = this.locks.get(BACK_OFF_SPIN_LOCK);
                        ((BackOffSpinLock) this.adaptiveLock).adapt(false);
                    }
                } catch (Exception e) {
                    //ignore
                } finally {
                    this.state.compareAndSet(false, true);
                }
            }
        }
    }

    public List<AdaptiveBackOffSpinLock> getLocks() {
        return (List<AdaptiveBackOffSpinLock>) this.locks.values();
    }

    public void setLocks(Map<String, AdaptiveBackOffSpinLock> locks) {
        this.locks = locks;
    }

    public boolean getState() {
        return this.state.get();
    }

    public void setState(boolean state) {
        this.state.set(state);
    }

    public AdaptiveBackOffSpinLock getAdaptiveLock() {
        return adaptiveLock;
    }

    public List<AtomicInteger> getTpsTable() {
        return tpsTable;
    }

    public void setSwapCriticalPoint(int swapCriticalPoint) {
        this.swapCriticalPoint = swapCriticalPoint;
    }

    public int getSwapCriticalPoint() {
        return swapCriticalPoint;
    }

    public boolean isOpen() {
        return this.isOpen.get();
    }

    public void setOpen(boolean open) {
        this.isOpen.set(open);
    }
}
