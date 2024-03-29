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
package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.AbstractBrokerRunnable;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

/**
 * 消费者 Id 变化监听器
 * Broker 端根据消费者上报的心跳判断消费者是否发生变化
 * 如发生变化，向消费者发送重平衡请求
 */
public class DefaultConsumerIdsChangeListener implements ConsumerIdsChangeListener {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;
    private final int cacheSize = 8096;

    private final ScheduledExecutorService scheduledExecutorService =  new ScheduledThreadPoolExecutor(1,
        ThreadUtils.newGenericThreadFactory("DefaultConsumerIdsChangeListener", true));

    private ConcurrentHashMap<String,List<Channel>> consumerChannelMap = new ConcurrentHashMap<>(cacheSize);

    public DefaultConsumerIdsChangeListener(BrokerController brokerController) {
        this.brokerController = brokerController;

        scheduledExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(brokerController.getBrokerConfig()) {
            @Override
            public void run0() {
                try {
                    notifyConsumerChange();
                } catch (Exception e) {
                    log.error(
                        "DefaultConsumerIdsChangeListen#notifyConsumerChange: unexpected error occurs", e);
                }
            }
        }, 30, 15, TimeUnit.SECONDS);
    }

    /**
     * 处理消费者 Id 变化事件
     *
     * @param event 事件类型
     * @param group 消费组名称
     * @param args 事件参数，事件为 {@link ConsumerGroupEvent#CHANGE} 时为 {@link List<Channel>}，表示消费组中所有消费者的 Channel；事件为 {@link ConsumerGroupEvent#REGISTER} 时为 {@link Collection<SubscriptionData>}，表示消费组中所有订阅信息
     */
    @Override
    public void handle(ConsumerGroupEvent event, String group, Object... args) {
        if (event == null) {
            return;
        }
        switch (event) {
            case CHANGE:
                // 如果发生变化，向所有消费者发送重平衡请求
                if (args == null || args.length < 1) {
                    return;
                }
                // 获取消费组中所有消费者的 Channel
                List<Channel> channels = (List<Channel>) args[0];
                if (channels != null && brokerController.getBrokerConfig().isNotifyConsumerIdsChangedEnable()) {
                    // 如果开启了 Broker 级别的重平衡主动通知，向消费组中所有消费者发送重平衡通知
                    if (this.brokerController.getBrokerConfig().isRealTimeNotifyConsumerChange()) {
                        // 实时通知
                        for (Channel chl : channels) {
                            // 向客户端发送重平衡请求
                            this.brokerController.getBroker2Client().notifyConsumerIdsChanged(chl, group);
                        }
                    } else {
                        // 放入缓存，定时通知
                        consumerChannelMap.put(group, channels);
                    }
                }
                break;
            case UNREGISTER:
                this.brokerController.getConsumerFilterManager().unRegister(group);
                break;
            case REGISTER:
                // 如果是新消费组注册，注册消费组订阅信息
                if (args == null || args.length < 1) {
                    return;
                }
                Collection<SubscriptionData> subscriptionDataList = (Collection<SubscriptionData>) args[0];
                this.brokerController.getConsumerFilterManager().register(group, subscriptionDataList);
                break;
            case CLIENT_REGISTER:
            case CLIENT_UNREGISTER:
                break;
            default:
                throw new RuntimeException("Unknown event " + event);
        }
    }

    private void notifyConsumerChange() {

        if (consumerChannelMap.isEmpty()) {
            return;
        }

        ConcurrentHashMap<String, List<Channel>> processMap = new ConcurrentHashMap<>(consumerChannelMap);
        consumerChannelMap = new ConcurrentHashMap<>(cacheSize);

        for (Map.Entry<String, List<Channel>> entry : processMap.entrySet()) {
            String consumerId = entry.getKey();
            List<Channel> channelList = entry.getValue();
            try {
                if (channelList != null && brokerController.getBrokerConfig().isNotifyConsumerIdsChangedEnable()) {
                    for (Channel chl : channelList) {
                        this.brokerController.getBroker2Client().notifyConsumerIdsChanged(chl, consumerId);
                    }
                }
            } catch (Exception e) {
                log.error("Failed to notify consumer when some consumers changed, consumerId to notify: {}",
                    consumerId, e);
            }
        }
    }

    @Override
    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }
}
