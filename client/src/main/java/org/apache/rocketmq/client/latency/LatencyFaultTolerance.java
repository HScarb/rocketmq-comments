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

package org.apache.rocketmq.client.latency;

/**
 * 故障延迟容错机制，更新不可用的 Broker，选择可用的 Broker
 *
 * @param <T>
 */
public interface LatencyFaultTolerance<T> {
    /**
     * 更新失败条目（Broker 名称）
     *
     * @param name Broker 名称
     * @param currentLatency 消息发送延迟时间
     * @param notAvailableDuration 规避 Broker 的时长
     */
    void updateFaultItem(final T name, final long currentLatency, final long notAvailableDuration);

    /**
     * 判断 Broker 是否可用
     *
     * @param name Broker 名称
     * @return true：可用，false：不可用
     */
    boolean isAvailable(final T name);

    /**
     * 从规避的 Broker 中移除
     *
     * @param name Broker 名称
     */
    void remove(final T name);

    /**
     * 尝试从规避的 Broker 中选择一个可用的 Broker，如果没有找到，返回 null
     *
     * @return
     */
    T pickOneAtLeast();
}
