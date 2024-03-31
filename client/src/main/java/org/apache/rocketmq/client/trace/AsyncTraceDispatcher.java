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
package org.apache.rocketmq.client.trace;

import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.RPCHook;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.rocketmq.client.trace.TraceConstants.TRACE_INSTANCE_NAME;

/**
 * 消息轨迹异步转发器，异步实现消息轨迹数据的发送
 */
public class AsyncTraceDispatcher implements TraceDispatcher {
    private static final Logger log = LoggerFactory.getLogger(AsyncTraceDispatcher.class);
    private static final AtomicInteger COUNTER = new AtomicInteger();
    private static final AtomicInteger INSTANCE_NUM = new AtomicInteger(0);
    private volatile boolean stopped = false;
    private final int traceInstanceId = INSTANCE_NUM.getAndIncrement();
    // 一次发送的请求包含数据条数
    private final int batchNum;
    // 一次发送最大消息大小，默认 128k
    private final int maxMsgSize;
    private final DefaultMQProducer traceProducer;
    // 丢弃的消息个数
    private AtomicLong discardCount;
    // 工作线程，从追加队列中获取一批待发送的消息轨迹数据，提交到线程池中执行
    private Thread worker;
    private final ThreadPoolExecutor traceExecutor;
    // 消息轨迹待发送数据队列，存储每个消息轨迹的上下文
    private final ArrayBlockingQueue<TraceContext> traceContextQueue;
    // 线程池内部队列，存储线程池发送任务
    private final ArrayBlockingQueue<Runnable> appenderQueue;
    private volatile Thread shutDownHook;

    private DefaultMQProducerImpl hostProducer;
    // 消费者信息，记录消费时的轨迹
    private DefaultMQPushConsumerImpl hostConsumer;
    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();
    private volatile String traceTopicName;
    private AtomicBoolean isStarted = new AtomicBoolean(false);
    private volatile AccessChannel accessChannel = AccessChannel.LOCAL;
    private String group;
    private Type type;
    private String namespaceV2;
    private final int flushTraceInterval = 5000;

    private long lastFlushTime = System.currentTimeMillis();

    public AsyncTraceDispatcher(String group, Type type, int batchNum, String traceTopicName, RPCHook rpcHook) {
        this.batchNum = Math.min(batchNum, 20);/* max value 20*/
        this.maxMsgSize = 128000;
        this.discardCount = new AtomicLong(0L);
        this.traceContextQueue = new ArrayBlockingQueue<>(2048);
        this.group = group;
        this.type = type;
        this.appenderQueue = new ArrayBlockingQueue<>(2048);
        if (!UtilAll.isBlank(traceTopicName)) {
            this.traceTopicName = traceTopicName;
        } else {
            this.traceTopicName = TopicValidator.RMQ_SYS_TRACE_TOPIC;
        }
        this.traceExecutor = new ThreadPoolExecutor(//
            2, //
            4, //
            1000 * 60, //
            TimeUnit.MILLISECONDS, //
            this.appenderQueue, //
            new ThreadFactoryImpl("MQTraceSendThread_" + traceInstanceId + "_"));
        traceProducer = getAndCreateTraceProducer(rpcHook);
    }

    public AccessChannel getAccessChannel() {
        return accessChannel;
    }

    public void setAccessChannel(AccessChannel accessChannel) {
        this.accessChannel = accessChannel;
    }

    public String getTraceTopicName() {
        return traceTopicName;
    }

    public void setTraceTopicName(String traceTopicName) {
        this.traceTopicName = traceTopicName;
    }

    public DefaultMQProducer getTraceProducer() {
        return traceProducer;
    }

    public DefaultMQProducerImpl getHostProducer() {
        return hostProducer;
    }

    public void setHostProducer(DefaultMQProducerImpl hostProducer) {
        this.hostProducer = hostProducer;
    }

    public DefaultMQPushConsumerImpl getHostConsumer() {
        return hostConsumer;
    }

    public void setHostConsumer(DefaultMQPushConsumerImpl hostConsumer) {
        this.hostConsumer = hostConsumer;
    }

    public String getNamespaceV2() {
        return namespaceV2;
    }

    public void setNamespaceV2(String namespaceV2) {
        this.namespaceV2 = namespaceV2;
    }

    public void start(String nameSrvAddr, AccessChannel accessChannel) throws MQClientException {
        // 使用 CAS 机制避免 start 方法重复执行
        if (isStarted.compareAndSet(false, true)) {
            traceProducer.setNamesrvAddr(nameSrvAddr);
            traceProducer.setInstanceName(TRACE_INSTANCE_NAME + "_" + nameSrvAddr);
            traceProducer.setNamespaceV2(namespaceV2);
            traceProducer.setEnableTrace(false);
            traceProducer.start();
        }
        this.accessChannel = accessChannel;
        // 启动后台工作线程，
        this.worker = new ThreadFactoryImpl("MQ-AsyncArrayDispatcher-Thread" + traceInstanceId, true)
            .newThread(new AsyncRunnable());
        this.worker.setDaemon(true);
        this.worker.start();
        this.registerShutDownHook();
    }

    private DefaultMQProducer getAndCreateTraceProducer(RPCHook rpcHook) {
        DefaultMQProducer traceProducerInstance = this.traceProducer;
        if (traceProducerInstance == null) {
            traceProducerInstance = new DefaultMQProducer(rpcHook);
            traceProducerInstance.setProducerGroup(genGroupNameForTrace());
            traceProducerInstance.setSendMsgTimeout(5000);
            traceProducerInstance.setVipChannelEnabled(false);
            // The max size of message is 128K
            traceProducerInstance.setMaxMessageSize(maxMsgSize);
        }
        return traceProducerInstance;
    }

    private String genGroupNameForTrace() {
        return TraceConstants.GROUP_NAME_PREFIX + "-" + this.group + "-" + this.type + "-" + COUNTER.incrementAndGet();
    }

    @Override
    public boolean append(final Object ctx) {
        boolean result = traceContextQueue.offer((TraceContext) ctx);
        if (!result) {
            log.info("buffer full" + discardCount.incrementAndGet() + " ,context is " + ctx);
        }
        return result;
    }

    @Override
    public void flush() {
        long end = System.currentTimeMillis() + 500;
        while (traceContextQueue.size() > 0 || appenderQueue.size() > 0 && System.currentTimeMillis() <= end) {
            try {
                flushTraceContext(true);
            } catch (Throwable throwable) {
                log.error("flushTraceContext error", throwable);
            }
        }
        if (appenderQueue.size() > 0) {
            log.error("There are still some traces that haven't been sent " + traceContextQueue.size() + "   " + appenderQueue.size());
        }
    }

    @Override
    public void shutdown() {
        flush();
        this.traceExecutor.shutdown();
        if (isStarted.get()) {
            traceProducer.shutdown();
        }
        this.removeShutdownHook();
        stopped = true;
    }

    public void registerShutDownHook() {
        if (shutDownHook == null) {
            shutDownHook = new Thread(new Runnable() {
                private volatile boolean hasShutdown = false;

                @Override
                public void run() {
                    synchronized (this) {
                        if (!this.hasShutdown) {
                            flush();
                        }
                    }
                }
            }, "ShutdownHookMQTrace");
            Runtime.getRuntime().addShutdownHook(shutDownHook);
        }
    }

    public void removeShutdownHook() {
        if (shutDownHook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(shutDownHook);
            } catch (IllegalStateException e) {
                // ignore - VM is already shutting down
            }
        }
    }

    /**
     * 批量从待处理消息轨迹队列中取数据，封装成一个 AsyncAppenderRequest 异步发送请求，提交给发送线程池执行
     * 批量发送机制是为了提高效率
     */
    class AsyncRunnable implements Runnable {
        private volatile boolean stopped = false;

        @Override
        public void run() {
            while (!stopped) {
                try {
                    flushTraceContext(false);
                } catch (Throwable e) {
                    log.error("flushTraceContext error", e);
                }
            }
            if (AsyncTraceDispatcher.this.stopped) {
                this.stopped = true;
            }
        }
    }

    /**
     * 批量从等待处理的消息轨迹队列中获取数据，将一批数据封装成一个发送请求，提交给异步发送线程池执行
     * @param forceFlush
     * @throws InterruptedException
     */
    private void flushTraceContext(boolean forceFlush) throws InterruptedException {
        List<TraceContext> contextList = new ArrayList<>(batchNum);
        int size = traceContextQueue.size();
        if (size != 0) {
            if (forceFlush || size >= batchNum || System.currentTimeMillis() - lastFlushTime > flushTraceInterval) {
                for (int i = 0; i < batchNum; i++) {
                    TraceContext context = traceContextQueue.poll();
                    if (context != null) {
                        contextList.add(context);
                    } else {
                        break;
                    }
                }
                asyncSendTraceMessage(contextList);
                return;
            }
        }
        // To prevent an infinite loop, add a wait time between each two task executions
        Thread.sleep(5);
    }

    private void asyncSendTraceMessage(List<TraceContext> contextList) {
        AsyncDataSendTask request = new AsyncDataSendTask(contextList);
        traceExecutor.submit(request);
        lastFlushTime = System.currentTimeMillis();
    }

    /**
     * 异步发送请求
     */
    class AsyncDataSendTask implements Runnable {
        private final List<TraceContext> contextList;

        public AsyncDataSendTask(List<TraceContext> contextList) {
            this.contextList = contextList;
        }

        /**
         * 发送消息轨迹数据
         */
        @Override
        public void run() {
            sendTraceData(contextList);
        }

        public void sendTraceData(List<TraceContext> contextList) {
            Map<String, List<TraceTransferBean>> transBeanMap = new HashMap<>(16);
            String traceTopic;
            for (TraceContext context : contextList) {
                AccessChannel accessChannel = context.getAccessChannel();
                if (accessChannel == null) {
                    accessChannel = AsyncTraceDispatcher.this.accessChannel;
                }
                String currentRegionId = context.getRegionId();
                if (currentRegionId == null || context.getTraceBeans().isEmpty()) {
                    continue;
                }
                if (AccessChannel.CLOUD == accessChannel) {
                    traceTopic = TraceConstants.TRACE_TOPIC_PREFIX + currentRegionId;
                } else {
                    traceTopic = traceTopicName;
                }

                String topic = context.getTraceBeans().get(0).getTopic();
                String key = topic + TraceConstants.CONTENT_SPLITOR + traceTopic;
                List<TraceTransferBean> transBeanList = transBeanMap.computeIfAbsent(key, k -> new ArrayList<>());
                TraceTransferBean traceData = TraceDataEncoder.encoderFromContextBean(context);
                transBeanList.add(traceData);
            }
            for (Map.Entry<String, List<TraceTransferBean>> entry : transBeanMap.entrySet()) {
                String[] key = entry.getKey().split(String.valueOf(TraceConstants.CONTENT_SPLITOR));
                flushData(entry.getValue(), key[0], key[1]);
            }
        }

        private void flushData(List<TraceTransferBean> transBeanList, String topic, String traceTopic) {
            if (transBeanList.size() == 0) {
                return;
            }
            StringBuilder buffer = new StringBuilder(1024);
            int count = 0;
            Set<String> keySet = new HashSet<String>();
            for (TraceTransferBean bean : transBeanList) {
                keySet.addAll(bean.getTransKey());
                buffer.append(bean.getTransData());
                count++;
                if (buffer.length() >= traceProducer.getMaxMessageSize()) {
                    sendTraceDataByMQ(keySet, buffer.toString(), traceTopic);
                    buffer.delete(0, buffer.length());
                    keySet.clear();
                    count = 0;
                }
            }
            if (count > 0) {
                sendTraceDataByMQ(keySet, buffer.toString(), traceTopic);
            }
            transBeanList.clear();
        }

        /**
         * Send message trace data
         *
         * @param keySet     the keyset in this batch(including msgId in original message not offsetMsgId)
         * @param data       the message trace data in this batch
         * @param traceTopic the topic which message trace data will send to
         */
        private void sendTraceDataByMQ(Set<String> keySet, final String data, String traceTopic) {
            final Message message = new Message(traceTopic, data.getBytes(StandardCharsets.UTF_8));
            // Keyset of message trace includes msgId of or original message
            message.setKeys(keySet);
            try {
                // 获取 traceTopic 的 Broker 列表，使用默认 Topic 的情况下只会有一个 Broker
                Set<String> traceBrokerSet = tryGetMessageQueueBrokerSet(traceProducer.getDefaultMQProducerImpl(), traceTopic);
                // 消息发送回调方法
                SendCallback callback = new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {

                    }

                    @Override
                    public void onException(Throwable e) {
                        log.error("send trace data failed, the traceData is {}", data, e);
                    }
                };
                if (traceBrokerSet.isEmpty()) {
                    // No cross set
                    traceProducer.send(message, callback, 5000);
                } else {
                    traceProducer.send(message, new MessageQueueSelector() {
                        // 队列选择方法，轮询
                        @Override
                        public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                            Set<String> brokerSet = (Set<String>) arg;
                            List<MessageQueue> filterMqs = new ArrayList<>();
                            for (MessageQueue queue : mqs) {
                                if (brokerSet.contains(queue.getBrokerName())) {
                                    filterMqs.add(queue);
                                }
                            }
                            int index = sendWhichQueue.incrementAndGet();
                            int pos = index % filterMqs.size();
                            return filterMqs.get(pos);
                        }
                    }, traceBrokerSet, callback);
                }

            } catch (Exception e) {
                log.error("send trace data failed, the traceData is {}", data, e);
            }
        }

        /**
         * 获取某 Topic 的 Broker 列表
         *
         * @param producer
         * @param topic
         * @return
         */
        private Set<String> tryGetMessageQueueBrokerSet(DefaultMQProducerImpl producer, String topic) {
            Set<String> brokerSet = new HashSet<>();
            TopicPublishInfo topicPublishInfo = producer.getTopicPublishInfoTable().get(topic);
            if (null == topicPublishInfo || !topicPublishInfo.ok()) {
                producer.getTopicPublishInfoTable().putIfAbsent(topic, new TopicPublishInfo());
                producer.getMqClientFactory().updateTopicRouteInfoFromNameServer(topic);
                topicPublishInfo = producer.getTopicPublishInfoTable().get(topic);
            }
            if (topicPublishInfo.isHaveTopicRouterInfo() || topicPublishInfo.ok()) {
                for (MessageQueue queue : topicPublishInfo.getMessageQueueList()) {
                    brokerSet.add(queue.getBrokerName());
                }
            }
            return brokerSet;
        }
    }

}