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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 支持自适应退避的自旋锁，{@link BackOffSpinLock#adapt(boolean)} 方法会自适应调整自旋次数（默认调整方式为翻倍或增加 1000）
 */
public class BackOffSpinLock implements AdaptiveBackOffSpinLock {

    /**
     * true：可用，false：已被占用
     */
    private AtomicBoolean putMessageSpinLock = new AtomicBoolean(true);

    /**
     * 当前允许的最大自旋次数
     */
    private int optimalDegree;

    /**
     * 初始自旋次数，默认 1000 次
     */
    private final static int INITIAL_DEGREE = 1000;

    /**
     * 最大自旋次数，默认 10000 次，防止自旋次数过大影响性能
     */
    private final static int MAX_OPTIMAL_DEGREE = 10000;

    /**
     * 退避次数，采用双槽机制，基于当前秒数的奇偶性，实现读写分离
     */
    private final List<AtomicInteger> numberOfRetreat;

    public BackOffSpinLock() {
        this.optimalDegree = INITIAL_DEGREE;

        numberOfRetreat = new ArrayList<>(2);
        numberOfRetreat.add(new AtomicInteger(0));
        numberOfRetreat.add(new AtomicInteger(0));
    }

    @Override
    public void lock() {
        int spinDegree = this.optimalDegree;
        while (true) {
            // 自旋获取锁，直到超过当前最大自旋次数
            for (int i = 0; i < spinDegree; i++) {
                if (this.putMessageSpinLock.compareAndSet(true, false)) {
                    return;
                }
            }
            // 自旋失败，增加当前描述槽位的退避计数器
            numberOfRetreat.get(LocalTime.now().getSecond() % 2).getAndIncrement();
            try {
                // 退避操作，线程短暂休眠，让出 CPU 时间片，让其他线程（尤其持有锁的线程）更多的执行机会
                Thread.sleep(0);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void unlock() {
        this.putMessageSpinLock.compareAndSet(false, true);
    }

    /**
     * 根据配置信息更新自旋次数
     */
    @Override
    public void update(MessageStoreConfig messageStoreConfig) {
        this.optimalDegree = messageStoreConfig.getSpinLockCollisionRetreatOptimalDegree();
    }

    public int getOptimalDegree() {
        return this.optimalDegree;
    }

    public void setOptimalDegree(int optimalDegree) {
        this.optimalDegree = optimalDegree;
    }

    /**
     * 判断是否可以调整自旋次数（在未达到最大自旋次数时可以往上调整）
     */
    public boolean isAdapt() {
        return optimalDegree < MAX_OPTIMAL_DEGREE;
    }

    /**
     * 自适应地调整自旋次数
     * @param isRise 增加/减少
     */
    public synchronized void adapt(boolean isRise) {
        if (isRise) {
            if (optimalDegree * 2 <= MAX_OPTIMAL_DEGREE) {
                // 如果翻倍了都未超过最大值，翻倍
                optimalDegree *= 2;
            } else {
                // 增加 1000
                if (optimalDegree + INITIAL_DEGREE <= MAX_OPTIMAL_DEGREE) {
                    optimalDegree += INITIAL_DEGREE;
                }
            }
        } else {
            // 减少 1000
            if (optimalDegree >= 2 * INITIAL_DEGREE) {
                optimalDegree -= INITIAL_DEGREE;
            }
        }
    }

    /**
     * 获取指定槽位的退避次数
     */
    public int getNumberOfRetreat(int pos) {
        return numberOfRetreat.get(pos).get();
    }

    /**
     * 设置指定槽位的退避次数
     */
    public void setNumberOfRetreat(int pos, int size) {
        this.numberOfRetreat.get(pos).set(size);
    }
}
