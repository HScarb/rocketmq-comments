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
package org.apache.rocketmq.broker.topic;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.StringUtils;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.TopicAttributes;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.Attribute;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.body.KVTable;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Broker 上的 Topic 管理器，管理 Topic 配置
 * Broker 会定时将 Topic 信息由心跳上报到 NameServer
 */
public class TopicConfigManager extends ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final long LOCK_TIMEOUT_MILLIS = 3000;
    private static final int SCHEDULE_TOPIC_QUEUE_NUM = 18;

    private transient final Lock topicConfigTableLock = new ReentrantLock();
    // Topic 配置表，存放所有 Topic 和配置信息
    private ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>(1024);
    // Topic 数据版本号
    private DataVersion dataVersion = new DataVersion();
    private transient BrokerController brokerController;

    public TopicConfigManager() {
    }

    public TopicConfigManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        {
            String topic = TopicValidator.RMQ_SYS_SELF_TEST_TOPIC;
            TopicConfig topicConfig = new TopicConfig(topic);
            TopicValidator.addSystemTopic(topic);
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {
            // 如果允许自动创建 Topic
            if (this.brokerController.getBrokerConfig().isAutoCreateTopicEnable()) {
                // 初始化用来被自动创建 Topic 继承的默认 Topic
                // 自动创建 Topic 创建前在客户端没有路由信息，会先从 NameServer 查询到这个默认 Topic 的路由信息
                String topic = TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC;
                TopicConfig topicConfig = new TopicConfig(topic);
                TopicValidator.addSystemTopic(topic);
                topicConfig.setReadQueueNums(this.brokerController.getBrokerConfig()
                    .getDefaultTopicQueueNums());
                topicConfig.setWriteQueueNums(this.brokerController.getBrokerConfig()
                    .getDefaultTopicQueueNums());
                // 权限：增加可继承
                int perm = PermName.PERM_INHERIT | PermName.PERM_READ | PermName.PERM_WRITE;
                topicConfig.setPerm(perm);
                // 将默认 Topic 加入 Topic 配置表
                this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
            }
        }
        {
            // 性能测试 Topic，用来被性能测试 example 代码调用
            String topic = TopicValidator.RMQ_SYS_BENCHMARK_TOPIC;
            TopicConfig topicConfig = new TopicConfig(topic);
            TopicValidator.addSystemTopic(topic);
            topicConfig.setReadQueueNums(1024);
            topicConfig.setWriteQueueNums(1024);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {
            String topic = this.brokerController.getBrokerConfig().getBrokerClusterName();
            TopicConfig topicConfig = new TopicConfig(topic);
            TopicValidator.addSystemTopic(topic);
            int perm = PermName.PERM_INHERIT;
            if (this.brokerController.getBrokerConfig().isClusterTopicEnable()) {
                perm |= PermName.PERM_READ | PermName.PERM_WRITE;
            }
            topicConfig.setPerm(perm);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {

            String topic = this.brokerController.getBrokerConfig().getBrokerName();
            TopicConfig topicConfig = new TopicConfig(topic);
            TopicValidator.addSystemTopic(topic);
            int perm = PermName.PERM_INHERIT;
            if (this.brokerController.getBrokerConfig().isBrokerTopicEnable()) {
                perm |= PermName.PERM_READ | PermName.PERM_WRITE;
            }
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            topicConfig.setPerm(perm);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {
            String topic = TopicValidator.RMQ_SYS_OFFSET_MOVED_EVENT;
            TopicConfig topicConfig = new TopicConfig(topic);
            TopicValidator.addSystemTopic(topic);
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {
            String topic = TopicValidator.RMQ_SYS_SCHEDULE_TOPIC;
            TopicConfig topicConfig = new TopicConfig(topic);
            TopicValidator.addSystemTopic(topic);
            topicConfig.setReadQueueNums(SCHEDULE_TOPIC_QUEUE_NUM);
            topicConfig.setWriteQueueNums(SCHEDULE_TOPIC_QUEUE_NUM);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {
            if (this.brokerController.getBrokerConfig().isTraceTopicEnable()) {
                String topic = this.brokerController.getBrokerConfig().getMsgTraceTopicName();
                TopicConfig topicConfig = new TopicConfig(topic);
                TopicValidator.addSystemTopic(topic);
                topicConfig.setReadQueueNums(1);
                topicConfig.setWriteQueueNums(1);
                this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
            }
        }
        {
            String topic = this.brokerController.getBrokerConfig().getBrokerClusterName() + "_" + MixAll.REPLY_TOPIC_POSTFIX;
            TopicConfig topicConfig = new TopicConfig(topic);
            TopicValidator.addSystemTopic(topic);
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {
            // PopAckConstants.REVIVE_TOPIC
            String topic = PopAckConstants.buildClusterReviveTopic(this.brokerController.getBrokerConfig().getBrokerClusterName());
            TopicConfig topicConfig = new TopicConfig(topic);
            TopicValidator.addSystemTopic(topic);
            topicConfig.setReadQueueNums(this.brokerController.getBrokerConfig().getReviveQueueNum());
            topicConfig.setWriteQueueNums(this.brokerController.getBrokerConfig().getReviveQueueNum());
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {
            // sync broker member group topic
            String topic = TopicValidator.SYNC_BROKER_MEMBER_GROUP_PREFIX + this.brokerController.getBrokerConfig().getBrokerName();
            TopicConfig topicConfig = new TopicConfig(topic);
            TopicValidator.addSystemTopic(topic);
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            topicConfig.setPerm(PermName.PERM_INHERIT);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {
            // TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC
            String topic = TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC;
            TopicConfig topicConfig = new TopicConfig(topic);
            TopicValidator.addSystemTopic(topic);
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }

        {
            // TopicValidator.RMQ_SYS_TRANS_OP_HALF_TOPIC
            String topic = TopicValidator.RMQ_SYS_TRANS_OP_HALF_TOPIC;
            TopicConfig topicConfig = new TopicConfig(topic);
            TopicValidator.addSystemTopic(topic);
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
    }

    public TopicConfig selectTopicConfig(final String topic) {
        return this.topicConfigTable.get(topic);
    }

    /**
     * 为 Topic 创建配置项
     * 在 {@link org.apache.rocketmq.broker.processor.SendMessageProcessor#sendMessage(ChannelHandlerContext, RemotingCommand, SendMessageContext, SendMessageRequestHeader)} 方法中创建 Topic
     * 实际是在 {@link org.apache.rocketmq.broker.processor.AbstractSendMessageProcessor#msgCheck(ChannelHandlerContext, SendMessageRequestHeader, RemotingCommand)} 方法中被调用
     * 会将 Topic 保存到本地缓存 {@link TopicConfigManager#topicConfigTable}，随后上报到 NameServer
     *
     * @param topic
     * @param defaultTopic 消息发送的默认 Topic，默认为 {@link TopicValidator#AUTO_CREATE_TOPIC_KEY_TOPIC}，如果开启自动创建 Topic 则会自动新建 Topic
     * @param remoteAddress
     * @param clientDefaultTopicQueueNums
     * @param topicSysFlag
     * @return
     */
    public TopicConfig createTopicInSendMessageMethod(final String topic, final String defaultTopic,
        final String remoteAddress, final int clientDefaultTopicQueueNums, final int topicSysFlag) {
        TopicConfig topicConfig = null;
        boolean createNew = false;

        try {
            if (this.topicConfigTableLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    // 配置表中已经存在，直接返回
                    topicConfig = this.topicConfigTable.get(topic);
                    if (topicConfig != null) {
                        return topicConfig;
                    }

                    // 根据默认 Topic 获取 Topic 配置
                    TopicConfig defaultTopicConfig = this.topicConfigTable.get(defaultTopic);
                    if (defaultTopicConfig != null) {
                        // 如果默认 Topic 是自动创建 Topic 的 Key
                        if (defaultTopic.equals(TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC)) {
                            if (!this.brokerController.getBrokerConfig().isAutoCreateTopicEnable()) {
                                defaultTopicConfig.setPerm(PermName.PERM_READ | PermName.PERM_WRITE);
                            }
                        }
                        // 如果默认 Topic 支持继承
                        if (PermName.isInherited(defaultTopicConfig.getPerm())) {
                            // 创建新 Topic，继承默认 Topic 的配置信息
                            topicConfig = new TopicConfig(topic);

                            // 继承默认 Topic 创建新 Topic
                            int queueNums = Math.min(clientDefaultTopicQueueNums, defaultTopicConfig.getWriteQueueNums());

                            if (queueNums < 0) {
                                queueNums = 0;
                            }

                            topicConfig.setReadQueueNums(queueNums);
                            topicConfig.setWriteQueueNums(queueNums);
                            // 删除子 Topic 的继承权限
                            int perm = defaultTopicConfig.getPerm();
                            perm &= ~PermName.PERM_INHERIT;
                            topicConfig.setPerm(perm);
                            topicConfig.setTopicSysFlag(topicSysFlag);
                            topicConfig.setTopicFilterType(defaultTopicConfig.getTopicFilterType());
                        } else {
                            // 默认 Topic 不存在，不允许自动创建 Topic
                            log.warn("Create new topic failed, because the default topic[{}] has no perm [{}] producer:[{}]",
                                defaultTopic, defaultTopicConfig.getPerm(), remoteAddress);
                        }
                    } else {
                        // 默认 Topic 不存在
                        log.warn("Create new topic failed, because the default topic[{}] not exist. producer:[{}]",
                            defaultTopic, remoteAddress);
                    }

                    if (topicConfig != null) {
                        log.info("Create new topic by default topic:[{}] config:[{}] producer:[{}]",
                            defaultTopic, topicConfig, remoteAddress);

                        // 将创建的默认 Topic 加入到内存
                        this.topicConfigTable.put(topic, topicConfig);
                        // 当前 TopicConfigManager 版本更新
                        long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
                        dataVersion.nextVersion(stateMachineVersion);

                        createNew = true;

                        // 持久化 Topic 配置文件
                        this.persist();
                    }
                } finally {
                    this.topicConfigTableLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("createTopicInSendMessageMethod exception", e);
        }

        // 如果创建了新 Topic 配置项，更新到 NameServer
        if (createNew) {
            this.brokerController.registerBrokerAll(false, true, true);
        }

        return topicConfig;
    }

    public TopicConfig createTopicIfAbsent(TopicConfig topicConfig) {
        return createTopicIfAbsent(topicConfig, true);
    }

    public TopicConfig createTopicIfAbsent(TopicConfig topicConfig, boolean register) {
        boolean createNew = false;
        if (topicConfig == null) {
            throw new NullPointerException("TopicConfig");
        }
        if (StringUtils.isEmpty(topicConfig.getTopicName())) {
            throw new IllegalArgumentException("TopicName");
        }

        try {
            if (this.topicConfigTableLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    TopicConfig existedTopicConfig = this.topicConfigTable.get(topicConfig.getTopicName());
                    if (existedTopicConfig != null) {
                        return existedTopicConfig;
                    }
                    log.info("Create new topic [{}] config:[{}]", topicConfig.getTopicName(), topicConfig);
                    this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
                    this.dataVersion.nextVersion();
                    createNew = true;
                    this.persist();
                } finally {
                    this.topicConfigTableLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("createTopicIfAbsent ", e);
        }
        if (createNew && register) {
            this.brokerController.registerIncrementBrokerData(topicConfig, dataVersion);
        }
        return this.topicConfigTable.get(topicConfig.getTopicName());
    }

    /**
     * 创建消费者消费失败发回消息的 Topic
     *
     * @param topic
     * @param clientDefaultTopicQueueNums
     * @param perm
     * @param topicSysFlag
     * @return
     */
    public TopicConfig createTopicInSendMessageBackMethod(
        final String topic,
        final int clientDefaultTopicQueueNums,
        final int perm,
        final int topicSysFlag) {
        return createTopicInSendMessageBackMethod(topic, clientDefaultTopicQueueNums, perm, false, topicSysFlag);
    }

    public TopicConfig createTopicInSendMessageBackMethod(
        final String topic,
        final int clientDefaultTopicQueueNums,
        final int perm,
        final boolean isOrder,
        final int topicSysFlag) {
        TopicConfig topicConfig = this.topicConfigTable.get(topic);
        if (topicConfig != null) {
            if (isOrder != topicConfig.isOrder()) {
                topicConfig.setOrder(isOrder);
                this.updateTopicConfig(topicConfig);
            }
            return topicConfig;
        }

        boolean createNew = false;

        try {
            if (this.topicConfigTableLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    topicConfig = this.topicConfigTable.get(topic);
                    if (topicConfig != null) {
                        return topicConfig;
                    }

                    topicConfig = new TopicConfig(topic);
                    topicConfig.setReadQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setWriteQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setPerm(perm);
                    topicConfig.setTopicSysFlag(topicSysFlag);
                    topicConfig.setOrder(isOrder);

                    log.info("create new topic {}", topicConfig);
                    this.topicConfigTable.put(topic, topicConfig);
                    createNew = true;
                    long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
                    dataVersion.nextVersion(stateMachineVersion);
                    this.persist();
                } finally {
                    this.topicConfigTableLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("createTopicInSendMessageBackMethod exception", e);
        }

        if (createNew) {
            this.brokerController.registerBrokerAll(false, true, true);
        }

        return topicConfig;
    }

    public TopicConfig createTopicOfTranCheckMaxTime(final int clientDefaultTopicQueueNums, final int perm) {
        TopicConfig topicConfig = this.topicConfigTable.get(TopicValidator.RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC);
        if (topicConfig != null)
            return topicConfig;

        boolean createNew = false;

        try {
            if (this.topicConfigTableLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    topicConfig = this.topicConfigTable.get(TopicValidator.RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC);
                    if (topicConfig != null)
                        return topicConfig;

                    topicConfig = new TopicConfig(TopicValidator.RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC);
                    topicConfig.setReadQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setWriteQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setPerm(perm);
                    topicConfig.setTopicSysFlag(0);

                    log.info("create new topic {}", topicConfig);
                    this.topicConfigTable.put(TopicValidator.RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC, topicConfig);
                    createNew = true;
                    this.dataVersion.nextVersion();
                    this.persist();
                } finally {
                    this.topicConfigTableLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("create TRANS_CHECK_MAX_TIME_TOPIC exception", e);
        }

        if (createNew) {
            this.brokerController.registerBrokerAll(false, true, true);
        }

        return topicConfig;
    }

    public void updateTopicUnitFlag(final String topic, final boolean unit) {

        TopicConfig topicConfig = this.topicConfigTable.get(topic);
        if (topicConfig != null) {
            int oldTopicSysFlag = topicConfig.getTopicSysFlag();
            if (unit) {
                topicConfig.setTopicSysFlag(TopicSysFlag.setUnitFlag(oldTopicSysFlag));
            } else {
                topicConfig.setTopicSysFlag(TopicSysFlag.clearUnitFlag(oldTopicSysFlag));
            }

            log.info("update topic sys flag. oldTopicSysFlag={}, newTopicSysFlag={}", oldTopicSysFlag,
                topicConfig.getTopicSysFlag());

            this.topicConfigTable.put(topic, topicConfig);

            long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
            dataVersion.nextVersion(stateMachineVersion);

            this.persist();
            this.brokerController.registerBrokerAll(false, true, true);
        }
    }

    public void updateTopicUnitSubFlag(final String topic, final boolean hasUnitSub) {
        TopicConfig topicConfig = this.topicConfigTable.get(topic);
        if (topicConfig != null) {
            int oldTopicSysFlag = topicConfig.getTopicSysFlag();
            if (hasUnitSub) {
                topicConfig.setTopicSysFlag(TopicSysFlag.setUnitSubFlag(oldTopicSysFlag));
            } else {
                topicConfig.setTopicSysFlag(TopicSysFlag.clearUnitSubFlag(oldTopicSysFlag));
            }

            log.info("update topic sys flag. oldTopicSysFlag={}, newTopicSysFlag={}", oldTopicSysFlag,
                topicConfig.getTopicSysFlag());

            this.topicConfigTable.put(topic, topicConfig);

            long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
            dataVersion.nextVersion(stateMachineVersion);

            this.persist();
            this.brokerController.registerBrokerAll(false, true, true);
        }
    }

    /**
     * 更新 Topic 元数据，增加版本号并持久化
     * @param topicConfig
     */
    public void updateTopicConfig(final TopicConfig topicConfig) {
        checkNotNull(topicConfig, "topicConfig shouldn't be null");

        Map<String, String> newAttributes = request(topicConfig);
        Map<String, String> currentAttributes = current(topicConfig.getTopicName());

        Map<String, String> finalAttributes = alterCurrentAttributes(
            this.topicConfigTable.get(topicConfig.getTopicName()) == null,
            ImmutableMap.copyOf(currentAttributes),
            ImmutableMap.copyOf(newAttributes));

        topicConfig.setAttributes(finalAttributes);

        TopicConfig old = this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        if (old != null) {
            log.info("update topic config, old:[{}] new:[{}]", old, topicConfig);
        } else {
            log.info("create new topic [{}]", topicConfig);
        }

        long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
        dataVersion.nextVersion(stateMachineVersion);

        this.persist(topicConfig.getTopicName(), topicConfig);
    }

    public void updateOrderTopicConfig(final KVTable orderKVTableFromNs) {

        if (orderKVTableFromNs != null && orderKVTableFromNs.getTable() != null) {
            boolean isChange = false;
            Set<String> orderTopics = orderKVTableFromNs.getTable().keySet();
            for (String topic : orderTopics) {
                TopicConfig topicConfig = this.topicConfigTable.get(topic);
                if (topicConfig != null && !topicConfig.isOrder()) {
                    topicConfig.setOrder(true);
                    isChange = true;
                    log.info("update order topic config, topic={}, order={}", topic, true);
                }
            }

            // We don't have a mandatory rule to maintain the validity of order conf in NameServer,
            // so we may overwrite the order field mistakenly.
            // To avoid the above case, we comment the below codes, please use mqadmin API to update
            // the order filed.
            /*for (Map.Entry<String, TopicConfig> entry : this.topicConfigTable.entrySet()) {
                String topic = entry.getKey();
                if (!orderTopics.contains(topic)) {
                    TopicConfig topicConfig = entry.getValue();
                    if (topicConfig.isOrder()) {
                        topicConfig.setOrder(false);
                        isChange = true;
                        log.info("update order topic config, topic={}, order={}", topic, false);
                    }
                }
            }*/

            if (isChange) {
                long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
                dataVersion.nextVersion(stateMachineVersion);
                this.persist();
            }
        }
    }

    // make it testable
    public Map<String, Attribute> allAttributes() {
        return TopicAttributes.ALL;
    }

    public boolean isOrderTopic(final String topic) {
        TopicConfig topicConfig = this.topicConfigTable.get(topic);
        if (topicConfig == null) {
            return false;
        } else {
            return topicConfig.isOrder();
        }
    }

    public void deleteTopicConfig(final String topic) {
        TopicConfig old = this.topicConfigTable.remove(topic);
        if (old != null) {
            log.info("delete topic config OK, topic: {}", old);
            this.dataVersion.nextVersion();
            this.persist();
        } else {
            log.warn("delete topic config failed, topic: {} not exists", topic);
        }
    }

    /**
     * 构造用于心跳请求体，将 Topic 配置和版本号由心跳包发往 NameServer
     * @return
     */
    public TopicConfigSerializeWrapper buildTopicConfigSerializeWrapper() {
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        topicConfigSerializeWrapper.setTopicConfigTable(this.topicConfigTable);
        DataVersion dataVersionCopy = new DataVersion();
        dataVersionCopy.assignNewOne(this.dataVersion);
        topicConfigSerializeWrapper.setDataVersion(dataVersionCopy);
        return topicConfigSerializeWrapper;
    }

    public void initStateVersion() {
        long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
        dataVersion.nextVersion(stateMachineVersion);
        this.persist();
    }

    @Override
    public String encode() {
        return encode(false);
    }

    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getTopicConfigPath(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            TopicConfigSerializeWrapper topicConfigSerializeWrapper =
                TopicConfigSerializeWrapper.fromJson(jsonString, TopicConfigSerializeWrapper.class);
            if (topicConfigSerializeWrapper != null) {
                this.topicConfigTable.putAll(topicConfigSerializeWrapper.getTopicConfigTable());
                this.dataVersion.assignNewOne(topicConfigSerializeWrapper.getDataVersion());
                this.printLoadDataWhenFirstBoot(topicConfigSerializeWrapper);
            }
        }
    }

    public String encode(final boolean prettyFormat) {
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        topicConfigSerializeWrapper.setTopicConfigTable(this.topicConfigTable);
        topicConfigSerializeWrapper.setDataVersion(this.dataVersion);
        return topicConfigSerializeWrapper.toJson(prettyFormat);
    }

    private void printLoadDataWhenFirstBoot(final TopicConfigSerializeWrapper tcs) {
        Iterator<Entry<String, TopicConfig>> it = tcs.getTopicConfigTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, TopicConfig> next = it.next();
            log.info("load exist local topic, {}", next.getValue().toString());
        }
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setTopicConfigTable(
        ConcurrentMap<String, TopicConfig> topicConfigTable) {
        this.topicConfigTable = topicConfigTable;
    }

    public ConcurrentMap<String, TopicConfig> getTopicConfigTable() {
        return topicConfigTable;
    }

    private Map<String, String> request(TopicConfig topicConfig) {
        return topicConfig.getAttributes() == null ? new HashMap<>() : topicConfig.getAttributes();
    }

    private Map<String, String> current(String topic) {
        TopicConfig topicConfig = this.topicConfigTable.get(topic);
        if (topicConfig == null) {
            return new HashMap<>();
        } else {
            Map<String, String> attributes = topicConfig.getAttributes();
            if (attributes == null) {
                return new HashMap<>();
            } else {
                return attributes;
            }
        }
    }

    private Map<String, String> alterCurrentAttributes(boolean create, ImmutableMap<String, String> currentAttributes,
        ImmutableMap<String, String> newAttributes) {
        Map<String, String> init = new HashMap<>();
        Map<String, String> add = new HashMap<>();
        Map<String, String> update = new HashMap<>();
        Map<String, String> delete = new HashMap<>();
        Set<String> keys = new HashSet<>();

        for (Entry<String, String> attribute : newAttributes.entrySet()) {
            String key = attribute.getKey();
            String realKey = realKey(key);
            String value = attribute.getValue();

            validate(realKey);
            duplicationCheck(keys, realKey);

            if (create) {
                if (key.startsWith("+")) {
                    init.put(realKey, value);
                } else {
                    throw new RuntimeException("only add attribute is supported while creating topic. key: " + realKey);
                }
            } else {
                if (key.startsWith("+")) {
                    if (!currentAttributes.containsKey(realKey)) {
                        add.put(realKey, value);
                    } else {
                        update.put(realKey, value);
                    }
                } else if (key.startsWith("-")) {
                    if (!currentAttributes.containsKey(realKey)) {
                        throw new RuntimeException("attempt to delete a nonexistent key: " + realKey);
                    }
                    delete.put(realKey, value);
                } else {
                    throw new RuntimeException("wrong format key: " + realKey);
                }
            }
        }

        validateAlter(init, true, false);
        validateAlter(add, false, false);
        validateAlter(update, false, false);
        validateAlter(delete, false, true);

        log.info("add: {}, update: {}, delete: {}", add, update, delete);
        HashMap<String, String> finalAttributes = new HashMap<>(currentAttributes);
        finalAttributes.putAll(init);
        finalAttributes.putAll(add);
        finalAttributes.putAll(update);
        for (String s : delete.keySet()) {
            finalAttributes.remove(s);
        }
        return finalAttributes;
    }

    private void duplicationCheck(Set<String> keys, String key) {
        boolean notExist = keys.add(key);
        if (!notExist) {
            throw new RuntimeException("alter duplication key. key: " + key);
        }
    }

    private void validate(String kvAttribute) {
        if (Strings.isNullOrEmpty(kvAttribute)) {
            throw new RuntimeException("kv string format wrong.");
        }

        if (kvAttribute.contains("+")) {
            throw new RuntimeException("kv string format wrong.");
        }

        if (kvAttribute.contains("-")) {
            throw new RuntimeException("kv string format wrong.");
        }
    }

    private void validateAlter(Map<String, String> alter, boolean init, boolean delete) {
        for (Entry<String, String> entry : alter.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            Attribute attribute = allAttributes().get(key);
            if (attribute == null) {
                throw new RuntimeException("unsupported key: " + key);
            }
            if (!init && !attribute.isChangeable()) {
                throw new RuntimeException("attempt to update an unchangeable attribute. key: " + key);
            }

            if (!delete) {
                attribute.verify(value);
            }
        }
    }

    private String realKey(String key) {
        return key.substring(1);
    }

    public boolean containsTopic(String topic) {
        return topicConfigTable.containsKey(topic);
    }
}
