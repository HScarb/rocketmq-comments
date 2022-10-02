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

package org.apache.rocketmq.common.stats;

import java.util.LinkedList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.InternalLogger;

/**
 * 监控数据指标项
 */
public class StatsItem {

    // 计数值
    private final LongAdder value = new LongAdder();

    // value 值变化的次数
    private final LongAdder times = new LongAdder();

    // 近 1min 内调用的快照信息，每 10s 采集一次，超过 6 个则淘汰最早入队的快照信息，长度不会超过 6
    private final LinkedList<CallSnapshot> csListMinute = new LinkedList<CallSnapshot>();

    // 近 1h 内调用的快照信息，每 10 min 采集一次，不会超过 6 个
    private final LinkedList<CallSnapshot> csListHour = new LinkedList<CallSnapshot>();

    // 近 1day 内调用的快照信息，每 1h 采集一次，不会超过 24 个
    private final LinkedList<CallSnapshot> csListDay = new LinkedList<CallSnapshot>();

    // 统计项名称，与 StatsItemSet 中的 statsName 相同
    private final String statsName;
    // 统计项 key，如具体 Topic 名称
    private final String statsKey;
    private final ScheduledExecutorService scheduledExecutorService;
    private final InternalLogger log;

    public StatsItem(String statsName, String statsKey, ScheduledExecutorService scheduledExecutorService,
        InternalLogger log) {
        this.statsName = statsName;
        this.statsKey = statsKey;
        this.scheduledExecutorService = scheduledExecutorService;
        this.log = log;
    }

    /**
     * 指标计算
     *
     * @param csList 采样容器
     * @return 指标快照
     */
    private static StatsSnapshot computeStatsData(final LinkedList<CallSnapshot> csList) {
        StatsSnapshot statsSnapshot = new StatsSnapshot();
        synchronized (csList) {
            double tps = 0;
            double avgpt = 0;
            long sum = 0;
            long timesDiff = 0;
            if (!csList.isEmpty()) {
                CallSnapshot first = csList.getFirst();
                CallSnapshot last = csList.getLast();
                sum = last.getValue() - first.getValue();
                tps = (sum * 1000.0d) / (last.getTimestamp() - first.getTimestamp());

                timesDiff = last.getTimes() - first.getTimes();
                if (timesDiff > 0) {
                    avgpt = (sum * 1.0d) / timesDiff;
                }
            }

            statsSnapshot.setSum(sum);
            statsSnapshot.setTps(tps);
            statsSnapshot.setAvgpt(avgpt);
            statsSnapshot.setTimes(timesDiff);
        }

        return statsSnapshot;
    }

    public StatsSnapshot getStatsDataInMinute() {
        return computeStatsData(this.csListMinute);
    }

    public StatsSnapshot getStatsDataInHour() {
        return computeStatsData(this.csListHour);
    }

    public StatsSnapshot getStatsDataInDay() {
        return computeStatsData(this.csListDay);
    }

    public void init() {

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    samplingInSeconds();
                } catch (Throwable ignored) {
                }
            }
        }, 0, 10, TimeUnit.SECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    samplingInMinutes();
                } catch (Throwable ignored) {
                }
            }
        }, 0, 10, TimeUnit.MINUTES);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    samplingInHour();
                } catch (Throwable ignored) {
                }
            }
        }, 0, 1, TimeUnit.HOURS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    printAtMinutes();
                } catch (Throwable ignored) {
                }
            }
        }, Math.abs(UtilAll.computeNextMinutesTimeMillis() - System.currentTimeMillis()), 1000 * 60, TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    printAtHour();
                } catch (Throwable ignored) {
                }
            }
        }, Math.abs(UtilAll.computeNextHourTimeMillis() - System.currentTimeMillis()), 1000 * 60 * 60, TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    printAtDay();
                } catch (Throwable ignored) {
                }
            }
        }, Math.abs(UtilAll.computeNextMorningTimeMillis() - System.currentTimeMillis()) - 2000, 1000 * 60 * 60 * 24, TimeUnit.MILLISECONDS);
    }

    /**
     * 对 10s 内指标进行采样，生成一个快照，加入列表（滑动窗口）
     */
    public void samplingInSeconds() {
        synchronized (this.csListMinute) {
            if (this.csListMinute.size() == 0) {
                this.csListMinute.add(new CallSnapshot(System.currentTimeMillis() - 10 * 1000, 0, 0));
            }
            this.csListMinute.add(new CallSnapshot(System.currentTimeMillis(), this.times.sum(), this.value
                .sum()));
            if (this.csListMinute.size() > 7) {
                this.csListMinute.removeFirst();
            }
        }
    }

    public void samplingInMinutes() {
        synchronized (this.csListHour) {
            if (this.csListHour.size() == 0) {
                this.csListHour.add(new CallSnapshot(System.currentTimeMillis() - 10 * 60 * 1000, 0, 0));
            }
            this.csListHour.add(new CallSnapshot(System.currentTimeMillis(), this.times.sum(), this.value
                .sum()));
            if (this.csListHour.size() > 7) {
                this.csListHour.removeFirst();
            }
        }
    }

    public void samplingInHour() {
        synchronized (this.csListDay) {
            if (this.csListDay.size() == 0) {
                this.csListDay.add(new CallSnapshot(System.currentTimeMillis() - 1 * 60 * 60 * 1000, 0, 0));
            }
            this.csListDay.add(new CallSnapshot(System.currentTimeMillis(), this.times.sum(), this.value
                .sum()));
            if (this.csListDay.size() > 25) {
                this.csListDay.removeFirst();
            }
        }
    }

    /**
     * 计算监控指标，输出
     */
    public void printAtMinutes() {
        StatsSnapshot ss = computeStatsData(this.csListMinute);
        log.info(String.format("[%s] [%s] Stats In One Minute, ", this.statsName, this.statsKey) + statPrintDetail(ss));
    }

    public void printAtHour() {
        StatsSnapshot ss = computeStatsData(this.csListHour);
        log.info(String.format("[%s] [%s] Stats In One Hour, ", this.statsName, this.statsKey) + statPrintDetail(ss));

    }

    public void printAtDay() {
        StatsSnapshot ss = computeStatsData(this.csListDay);
        log.info(String.format("[%s] [%s] Stats In One Day, ", this.statsName, this.statsKey) + statPrintDetail(ss));
    }

    protected String statPrintDetail(StatsSnapshot ss) {
        return String.format("SUM: %d TPS: %.2f AVGPT: %.2f",
                ss.getSum(),
                ss.getTps(),
                ss.getAvgpt());
    }

    public LongAdder getValue() {
        return value;
    }

    public String getStatsKey() {
        return statsKey;
    }

    public String getStatsName() {
        return statsName;
    }

    public LongAdder getTimes() {
        return times;
    }
}

/**
 * 统计快照
 */
class CallSnapshot {
    // 快照时间戳
    private final long timestamp;
    // 快照生成时 value 值变化的次数
    private final long times;
    // 快照生成时当前的统计值
    private final long value;

    public CallSnapshot(long timestamp, long times, long value) {
        super();
        this.timestamp = timestamp;
        this.times = times;
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getTimes() {
        return times;
    }

    public long getValue() {
        return value;
    }
}
