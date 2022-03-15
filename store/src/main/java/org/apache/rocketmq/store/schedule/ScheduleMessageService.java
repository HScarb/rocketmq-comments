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
package org.apache.rocketmq.store.schedule;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

/**
 * 延迟消息服务
 */
public class ScheduleMessageService extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private static final long FIRST_DELAY_TIME = 1000L;
    private static final long DELAY_FOR_A_WHILE = 100L;
    private static final long DELAY_FOR_A_PERIOD = 10000L;
    private static final long WAIT_FOR_SHUTDOWN = 5000L;
    private static final long DELAY_FOR_A_SLEEP = 10L;

    private final ConcurrentMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable =
        new ConcurrentHashMap<Integer, Long>(32);

    private final ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable =
        new ConcurrentHashMap<Integer, Long>(32);
    private final DefaultMessageStore defaultMessageStore;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private ScheduledExecutorService deliverExecutorService;
    private MessageStore writeMessageStore;
    private int maxDelayLevel;
    // 异步投递
    private boolean enableAsyncDeliver = false;
    // 异步投递线程池
    private ScheduledExecutorService handleExecutorService;
    // 异步投递任务等待队列
    private final Map<Integer /* level */, LinkedBlockingQueue<PutResultProcess>> deliverPendingTable =
        new ConcurrentHashMap<>(32);

    public ScheduleMessageService(final DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.writeMessageStore = defaultMessageStore;
        if (defaultMessageStore != null) {
            this.enableAsyncDeliver = defaultMessageStore.getMessageStoreConfig().isEnableScheduleAsyncDeliver();
        }
    }

    public static int queueId2DelayLevel(final int queueId) {
        return queueId + 1;
    }

    public static int delayLevel2QueueId(final int delayLevel) {
        return delayLevel - 1;
    }

    /**
     * @param writeMessageStore the writeMessageStore to set
     */
    public void setWriteMessageStore(MessageStore writeMessageStore) {
        this.writeMessageStore = writeMessageStore;
    }

    public void buildRunningStats(HashMap<String, String> stats) {
        Iterator<Map.Entry<Integer, Long>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Long> next = it.next();
            int queueId = delayLevel2QueueId(next.getKey());
            long delayOffset = next.getValue();
            long maxOffset = this.defaultMessageStore.getMaxOffsetInQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, queueId);
            String value = String.format("%d,%d", delayOffset, maxOffset);
            String key = String.format("%s_%d", RunningStats.scheduleMessageOffset.name(), next.getKey());
            stats.put(key, value);
        }
    }

    /**
     * 更新下一次要投递的逻辑偏移量
     *
     * @param delayLevel 延迟等级
     * @param offset 该延迟等级下次要投递的逻辑偏移量
     */
    private void updateOffset(int delayLevel, long offset) {
        this.offsetTable.put(delayLevel, offset);
    }

    public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
        Long time = this.delayLevelTable.get(delayLevel);
        if (time != null) {
            return time + storeTimestamp;
        }

        return storeTimestamp + 1000;
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            super.load();
            this.deliverExecutorService = new ScheduledThreadPoolExecutor(this.maxDelayLevel, new ThreadFactoryImpl("ScheduleMessageTimerThread_"));
            // 异步投递线程池
            if (this.enableAsyncDeliver) {
                this.handleExecutorService = new ScheduledThreadPoolExecutor(this.maxDelayLevel, new ThreadFactoryImpl("ScheduleMessageExecutorHandleThread_"));
            }
            // 为每个延迟等级创建一个DeliverDelayedMessageTimerTask，用于周期性扫描延迟等级的消息，将到期的消息重新投递
            for (Map.Entry<Integer, Long> entry : this.delayLevelTable.entrySet()) {
                Integer level = entry.getKey();
                Long timeDelay = entry.getValue();
                Long offset = this.offsetTable.get(level);
                if (null == offset) {
                    offset = 0L;
                }

                if (timeDelay != null) {
                    // 异步投递，
                    if (this.enableAsyncDeliver) {
                        this.handleExecutorService.schedule(new HandlePutResultTask(level), FIRST_DELAY_TIME, TimeUnit.MILLISECONDS);
                    }
                    // 同步投递，创建任务周期性扫描延迟等级的消息，将到期的消息重新投递
                    this.deliverExecutorService.schedule(new DeliverDelayedMessageTimerTask(level, offset), FIRST_DELAY_TIME, TimeUnit.MILLISECONDS);
                }
            }

            // 创建一个周期性定时任务，定时将offsetTable持久化
            this.deliverExecutorService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    try {
                        if (started.get()) {
                            ScheduleMessageService.this.persist();
                        }
                    } catch (Throwable e) {
                        log.error("scheduleAtFixedRate flush exception", e);
                    }
                }
            }, 10000, this.defaultMessageStore.getMessageStoreConfig().getFlushDelayOffsetInterval(), TimeUnit.MILLISECONDS);
        }
    }

    public void shutdown() {
        if (this.started.compareAndSet(true, false) && null != this.deliverExecutorService) {
            this.deliverExecutorService.shutdown();
            try {
                this.deliverExecutorService.awaitTermination(WAIT_FOR_SHUTDOWN, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                log.error("deliverExecutorService awaitTermination error", e);
            }

            if (this.handleExecutorService != null) {
                this.handleExecutorService.shutdown();
                try {
                    this.handleExecutorService.awaitTermination(WAIT_FOR_SHUTDOWN, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    log.error("handleExecutorService awaitTermination error", e);
                }
            }

            if (this.deliverPendingTable != null) {
                for (int i = 1; i <= this.deliverPendingTable.size(); i++) {
                    log.warn("deliverPendingTable level: {}, size: {}", i, this.deliverPendingTable.get(i).size());
                }
            }

            this.persist();
        }
    }

    public boolean isStarted() {
        return started.get();
    }

    public int getMaxDelayLevel() {
        return maxDelayLevel;
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    @Override
    public boolean load() {
        boolean result = super.load();
        result = result && this.parseDelayLevel();
        result = result && this.correctDelayOffset();
        return result;
    }

    public boolean correctDelayOffset() {
        try {
            for (int delayLevel : delayLevelTable.keySet()) {
                ConsumeQueue cq =
                    ScheduleMessageService.this.defaultMessageStore.findConsumeQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC,
                        delayLevel2QueueId(delayLevel));
                Long currentDelayOffset = offsetTable.get(delayLevel);
                if (currentDelayOffset == null || cq == null) {
                    continue;
                }
                long correctDelayOffset = currentDelayOffset;
                long cqMinOffset = cq.getMinOffsetInQueue();
                long cqMaxOffset = cq.getMaxOffsetInQueue();
                if (currentDelayOffset < cqMinOffset) {
                    correctDelayOffset = cqMinOffset;
                    log.error("schedule CQ offset invalid. offset={}, cqMinOffset={}, cqMaxOffset={}, queueId={}",
                        currentDelayOffset, cqMinOffset, cqMaxOffset, cq.getQueueId());
                }

                if (currentDelayOffset > cqMaxOffset) {
                    correctDelayOffset = cqMaxOffset;
                    log.error("schedule CQ offset invalid. offset={}, cqMinOffset={}, cqMaxOffset={}, queueId={}",
                        currentDelayOffset, cqMinOffset, cqMaxOffset, cq.getQueueId());
                }
                if (correctDelayOffset != currentDelayOffset) {
                    log.error("correct delay offset [ delayLevel {} ] from {} to {}", delayLevel, currentDelayOffset, correctDelayOffset);
                    offsetTable.put(delayLevel, correctDelayOffset);
                }
            }
        } catch (Exception e) {
            log.error("correctDelayOffset exception", e);
            return false;
        }
        return true;
    }

    @Override
    public String configFilePath() {
        return StorePathConfigHelper.getDelayOffsetStorePath(this.defaultMessageStore.getMessageStoreConfig()
            .getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            DelayOffsetSerializeWrapper delayOffsetSerializeWrapper =
                DelayOffsetSerializeWrapper.fromJson(jsonString, DelayOffsetSerializeWrapper.class);
            if (delayOffsetSerializeWrapper != null) {
                this.offsetTable.putAll(delayOffsetSerializeWrapper.getOffsetTable());
            }
        }
    }

    @Override
    public String encode(final boolean prettyFormat) {
        DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = new DelayOffsetSerializeWrapper();
        delayOffsetSerializeWrapper.setOffsetTable(this.offsetTable);
        return delayOffsetSerializeWrapper.toJson(prettyFormat);
    }

    /**
     * 从配置文件中解析延迟等级信息，并保存到延迟等级映射表中
     */
    public boolean parseDelayLevel() {
        HashMap<String, Long> timeUnitTable = new HashMap<String, Long>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);

        String levelString = this.defaultMessageStore.getMessageStoreConfig().getMessageDelayLevel();
        try {
            String[] levelArray = levelString.split(" ");
            for (int i = 0; i < levelArray.length; i++) {
                String value = levelArray[i];
                String ch = value.substring(value.length() - 1);
                Long tu = timeUnitTable.get(ch);

                int level = i + 1;
                if (level > this.maxDelayLevel) {
                    this.maxDelayLevel = level;
                }
                long num = Long.parseLong(value.substring(0, value.length() - 1));
                long delayTimeMillis = tu * num;
                this.delayLevelTable.put(level, delayTimeMillis);
                if (this.enableAsyncDeliver) {
                    this.deliverPendingTable.put(level, new LinkedBlockingQueue<>());
                }
            }
        } catch (Exception e) {
            log.error("parseDelayLevel exception", e);
            log.info("levelString String = {}", levelString);
            return false;
        }

        return true;
    }

    /**
     * 消息到期，把原Topic和QueueId恢复
     */
    private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());

        TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
        long tagsCodeValue =
            MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

        msgInner.setWaitStoreMsgOK(false);
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);

        msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));

        String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
        int queueId = Integer.parseInt(queueIdStr);
        msgInner.setQueueId(queueId);

        return msgInner;
    }

    /**
     * 定时消息扫描和投递任务，持续扫描一个delayLevel队列中的消息，如果到期则投递消息
     */
    class DeliverDelayedMessageTimerTask implements Runnable {
        private final int delayLevel;
        private final long offset;

        public DeliverDelayedMessageTimerTask(int delayLevel, long offset) {
            this.delayLevel = delayLevel;
            this.offset = offset;
        }

        @Override
        public void run() {
            try {
                if (isStarted()) {
                    this.executeOnTimeup();
                }
            } catch (Exception e) {
                // XXX: warn and notify me
                log.error("ScheduleMessageService, executeOnTimeup exception", e);
                this.scheduleNextTimerTask(this.offset, DELAY_FOR_A_PERIOD);
            }
        }

        /**
         * 纠正下次投递时间，如果时间特别大，则纠正为当前时间
         * 
         * @return
         */
        private long correctDeliverTimestamp(final long now, final long deliverTimestamp) {

            long result = deliverTimestamp;

            long maxTimestamp = now + ScheduleMessageService.this.delayLevelTable.get(this.delayLevel);
            if (deliverTimestamp > maxTimestamp) {
                result = now;
            }

            return result;
        }

        public void executeOnTimeup() {
            // 根据delayLevel查找对应的延迟消息ConsumeQueue
            ConsumeQueue cq =
                ScheduleMessageService.this.defaultMessageStore.findConsumeQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC,
                    delayLevel2QueueId(delayLevel));

            if (cq == null) {
                this.scheduleNextTimerTask(this.offset, DELAY_FOR_A_WHILE);
                return;
            }

            // 根据ConsumeQueue的有效延迟消息逻辑offset，获取所有有效的消息
            SelectMappedBufferResult bufferCQ = cq.getIndexBuffer(this.offset);
            if (bufferCQ == null) {
                long resetOffset;
                if ((resetOffset = cq.getMinOffsetInQueue()) > this.offset) {
                    log.error("schedule CQ offset invalid. offset={}, cqMinOffset={}, queueId={}",
                        this.offset, resetOffset, cq.getQueueId());
                } else if ((resetOffset = cq.getMaxOffsetInQueue()) < this.offset) {
                    log.error("schedule CQ offset invalid. offset={}, cqMaxOffset={}, queueId={}",
                        this.offset, resetOffset, cq.getQueueId());
                } else {
                    resetOffset = this.offset;
                }

                this.scheduleNextTimerTask(resetOffset, DELAY_FOR_A_WHILE);
                return;
            }

            long nextOffset = this.offset;
            try {
                int i = 0;
                ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                // 遍历ConsumeQueue中的所有有效消息
                for (; i < bufferCQ.getSize() && isStarted(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                    // 获取ConsumeQueue索引的三个关键属性
                    long offsetPy = bufferCQ.getByteBuffer().getLong();
                    int sizePy = bufferCQ.getByteBuffer().getInt();
                    long tagsCode = bufferCQ.getByteBuffer().getLong();

                    if (cq.isExtAddr(tagsCode)) {
                        if (cq.getExt(tagsCode, cqExtUnit)) {
                            tagsCode = cqExtUnit.getTagsCode();
                        } else {
                            //can't find ext content.So re compute tags code.
                            log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}",
                                tagsCode, offsetPy, sizePy);
                            long msgStoreTime = defaultMessageStore.getCommitLog().pickupStoreTimestamp(offsetPy, sizePy);
                            tagsCode = computeDeliverTimestamp(delayLevel, msgStoreTime);
                        }
                    }
                    // ConsumeQueue里面的tagsCode实际是一个时间点（投递时间点）
                    long now = System.currentTimeMillis();
                    long deliverTimestamp = this.correctDeliverTimestamp(now, tagsCode);
                    nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);

                    // 如果现在已经到了投递时间点，投递消息
                    // 如果现在还没到投递时间点，继续创建一个定时任务，countdown秒之后执行
                    long countdown = deliverTimestamp - now;
                    if (countdown > 0) {
                        this.scheduleNextTimerTask(nextOffset, DELAY_FOR_A_WHILE);
                        return;
                    }

                    MessageExt msgExt = ScheduleMessageService.this.defaultMessageStore.lookMessageByOffset(offsetPy, sizePy);
                    if (msgExt == null) {
                        continue;
                    }

                    MessageExtBrokerInner msgInner = ScheduleMessageService.this.messageTimeup(msgExt);
                    if (TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC.equals(msgInner.getTopic())) {
                        log.error("[BUG] the real topic of schedule msg is {}, discard the msg. msg={}",
                            msgInner.getTopic(), msgInner);
                        continue;
                    }
                    // 重新投递消息到CommitLog
                    boolean deliverSuc;
                    if (ScheduleMessageService.this.enableAsyncDeliver) {
                        // 异步投递
                        deliverSuc = this.asyncDeliver(msgInner, msgExt.getMsgId(), offset, offsetPy, sizePy);
                    } else {
                        // 同步投递
                        deliverSuc = this.syncDeliver(msgInner, msgExt.getMsgId(), offset, offsetPy, sizePy);
                    }

                    // 投递失败（流控、阻塞、投递异常等原因），等待0.1s再次执行投递任务
                    if (!deliverSuc) {
                        this.scheduleNextTimerTask(nextOffset, DELAY_FOR_A_WHILE);
                        return;
                    }
                }

                nextOffset = this.offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
            } catch (Exception e) {
                log.error("ScheduleMessageService, messageTimeup execute error, offset = {}", nextOffset, e);
            } finally {
                bufferCQ.release();
            }

            // 该条ConsumeQueue索引对应的消息如果未到投递时间，那么创建一个定时任务，到投递时间时执行
            // 如果有还未投递的消息，创建定时任务后直接返回
            this.scheduleNextTimerTask(nextOffset, DELAY_FOR_A_WHILE);
        }

        /**
         * 开启一个新的定时任务，判断和投递消息
         */
        public void scheduleNextTimerTask(long offset, long delay) {
            ScheduleMessageService.this.deliverExecutorService.schedule(new DeliverDelayedMessageTimerTask(
                this.delayLevel, offset), delay, TimeUnit.MILLISECONDS);
        }

        /**
         * 同步投递
         */
        private boolean syncDeliver(MessageExtBrokerInner msgInner, String msgId, long offset, long offsetPy,
            int sizePy) {
            PutResultProcess resultProcess = deliverMessage(msgInner, msgId, offset, offsetPy, sizePy, false);
            PutMessageResult result = resultProcess.get();
            boolean sendStatus = result != null && result.getPutMessageStatus() == PutMessageStatus.PUT_OK;
            if (sendStatus) {
                ScheduleMessageService.this.updateOffset(this.delayLevel, resultProcess.getNextOffset());
            }
            return sendStatus;
        }

        /**
         * 异步投递
         */
        private boolean asyncDeliver(MessageExtBrokerInner msgInner, String msgId, long offset, long offsetPy,
            int sizePy) {
            // 获取该延迟等级的异步投递任务队列
            Queue<PutResultProcess> processesQueue = ScheduleMessageService.this.deliverPendingTable.get(this.delayLevel);

            //Flow Control 流控
            int currentPendingNum = processesQueue.size();
            int maxPendingLimit = ScheduleMessageService.this.defaultMessageStore.getMessageStoreConfig()
                .getScheduleAsyncDeliverMaxPendingLimit();
            // 如果当前队列中进行中任务数量大于阈值，流控，返回投递失败
            if (currentPendingNum > maxPendingLimit) {
                log.warn("Asynchronous deliver triggers flow control, " +
                    "currentPendingNum={}, maxPendingLimit={}", currentPendingNum, maxPendingLimit);
                return false;
            }

            //Blocked 阻塞，因当前队列第一个任务重试次数过多导致
            PutResultProcess firstProcess = processesQueue.peek();
            if (firstProcess != null && firstProcess.need2Blocked()) {
                log.warn("Asynchronous deliver block. info={}", firstProcess.toString());
                return false;
            }

            // 投递消息，创建投递消息异步任务
            PutResultProcess resultProcess = deliverMessage(msgInner, msgId, offset, offsetPy, sizePy, true);
            // 将投递消息异步任务加入队列，等待异步投递线程处理
            processesQueue.add(resultProcess);
            return true;
        }

        /**
         * 创建投递消息异步任务
         */
        private PutResultProcess deliverMessage(MessageExtBrokerInner msgInner, String msgId, long offset,
            long offsetPy, int sizePy, boolean autoResend) {
            CompletableFuture<PutMessageResult> future =
                ScheduleMessageService.this.writeMessageStore.asyncPutMessage(msgInner);
            return new PutResultProcess()
                .setTopic(msgInner.getTopic())
                .setDelayLevel(this.delayLevel)
                .setOffset(offset)
                .setPhysicOffset(offsetPy)
                .setPhysicSize(sizePy)
                .setMsgId(msgId)
                .setAutoResend(autoResend)
                .setFuture(future)
                .thenProcess();
        }
    }

    /**
     * 异步投递消息任务状态更新任务
     */
    public class HandlePutResultTask implements Runnable {
        private final int delayLevel;

        public HandlePutResultTask(int delayLevel) {
            this.delayLevel = delayLevel;
        }

        @Override
        public void run() {
            LinkedBlockingQueue<PutResultProcess> pendingQueue =
                ScheduleMessageService.this.deliverPendingTable.get(this.delayLevel);

            PutResultProcess putResultProcess;
            // 循环获取队列中第一个投递任务，查看其执行状态并执行对应操作
            while ((putResultProcess = pendingQueue.peek()) != null) {
                try {
                    switch (putResultProcess.getStatus()) {
                        case SUCCESS:
                            // 消息投递成功，从队列中移除该投递任务
                            ScheduleMessageService.this.updateOffset(this.delayLevel, putResultProcess.getNextOffset());
                            pendingQueue.remove();
                            break;
                        case RUNNING:
                            // 正在投递，不做操作
                            break;
                        case EXCEPTION:
                            // 投递出错
                            if (!isStarted()) {
                                log.warn("HandlePutResultTask shutdown, info={}", putResultProcess.toString());
                                return;
                            }
                            log.warn("putResultProcess error, info={}", putResultProcess.toString());
                            putResultProcess.onException();
                            break;
                        case SKIP:
                            // 跳过，直接从队列中移除
                            log.warn("putResultProcess skip, info={}", putResultProcess.toString());
                            pendingQueue.remove();
                            break;
                    }
                } catch (Exception e) {
                    log.error("HandlePutResultTask exception. info={}", putResultProcess.toString(), e);
                    putResultProcess.onException();
                }
            }

            // 等待0.01s，继续下一次扫描
            if (isStarted()) {
                ScheduleMessageService.this.handleExecutorService
                    .schedule(new HandlePutResultTask(this.delayLevel), DELAY_FOR_A_SLEEP, TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * 延迟消息异步投递任务
     */
    public class PutResultProcess {
        private String topic;
        private long offset;
        private long physicOffset;
        private int physicSize;
        private int delayLevel;
        private String msgId;
        private boolean autoResend = false;
        private CompletableFuture<PutMessageResult> future;

        // 投递重试次数
        private volatile int resendCount = 0;
        // 任务执行状态
        private volatile ProcessStatus status = ProcessStatus.RUNNING;

        public PutResultProcess setTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public PutResultProcess setOffset(long offset) {
            this.offset = offset;
            return this;
        }

        public PutResultProcess setPhysicOffset(long physicOffset) {
            this.physicOffset = physicOffset;
            return this;
        }

        public PutResultProcess setPhysicSize(int physicSize) {
            this.physicSize = physicSize;
            return this;
        }

        public PutResultProcess setDelayLevel(int delayLevel) {
            this.delayLevel = delayLevel;
            return this;
        }

        public PutResultProcess setMsgId(String msgId) {
            this.msgId = msgId;
            return this;
        }

        public PutResultProcess setAutoResend(boolean autoResend) {
            this.autoResend = autoResend;
            return this;
        }

        public PutResultProcess setFuture(CompletableFuture<PutMessageResult> future) {
            this.future = future;
            return this;
        }

        public String getTopic() {
            return topic;
        }

        public long getOffset() {
            return offset;
        }

        public long getNextOffset() {
            return offset + 1;
        }

        public long getPhysicOffset() {
            return physicOffset;
        }

        public int getPhysicSize() {
            return physicSize;
        }

        public Integer getDelayLevel() {
            return delayLevel;
        }

        public String getMsgId() {
            return msgId;
        }

        public boolean isAutoResend() {
            return autoResend;
        }

        public CompletableFuture<PutMessageResult> getFuture() {
            return future;
        }

        public int getResendCount() {
            return resendCount;
        }

        public PutResultProcess thenProcess() {
            this.future.thenAccept(result -> {
                this.handleResult(result);
            });

            this.future.exceptionally(e -> {
                log.error("ScheduleMessageService put message exceptionally, info: {}",
                    PutResultProcess.this.toString(), e);

                onException();
                return null;
            });
            return this;
        }

        /**
         * 处理消息投递结果，成功则更新统计数据，失败则执行异常操作
         */
        private void handleResult(PutMessageResult result) {
            if (result != null && result.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                onSuccess(result);
            } else {
                log.warn("ScheduleMessageService put message failed. info: {}.", result);
                onException();
            }
        }

        public void onSuccess(PutMessageResult result) {
            this.status = ProcessStatus.SUCCESS;
            if (ScheduleMessageService.this.defaultMessageStore.getMessageStoreConfig().isEnableScheduleMessageStats()) {
                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incQueueGetNums(MixAll.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, delayLevel - 1, result.getAppendMessageResult().getMsgNum());
                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incQueueGetSize(MixAll.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, delayLevel - 1, result.getAppendMessageResult().getWroteBytes());
                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incGroupGetNums(MixAll.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, result.getAppendMessageResult().getMsgNum());
                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incGroupGetSize(MixAll.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, result.getAppendMessageResult().getWroteBytes());
                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incTopicPutNums(this.topic, result.getAppendMessageResult().getMsgNum(), 1);
                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incTopicPutSize(this.topic, result.getAppendMessageResult().getWroteBytes());
                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incBrokerPutNums(result.getAppendMessageResult().getMsgNum());
            }
        }

        /**
         * 消息投递异常，重试或跳过
         */
        public void onException() {
            log.warn("ScheduleMessageService onException, info: {}", this.toString());
            if (this.autoResend) {
                this.resend();
            } else {
                this.status = ProcessStatus.SKIP;
            }
        }

        public ProcessStatus getStatus() {
            return this.status;
        }

        public PutMessageResult get() {
            try {
                return this.future.get();
            } catch (InterruptedException | ExecutionException e) {
                return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null);
            }
        }

        /**
         * 重新投递消息
         */
        private void resend() {
            log.info("Resend message, info: {}", this.toString());

            // Gradually increase the resend interval.
            try {
                Thread.sleep(Math.min(this.resendCount++ * 100, 60 * 1000));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            try {
                // 查询消息
                MessageExt msgExt = ScheduleMessageService.this.defaultMessageStore.lookMessageByOffset(this.physicOffset, this.physicSize);
                if (msgExt == null) {
                    log.warn("ScheduleMessageService resend not found message. info: {}", this.toString());
                    this.status = need2Skip() ? ProcessStatus.SKIP : ProcessStatus.EXCEPTION;
                    return;
                }
                // 重投递消息（同步）
                MessageExtBrokerInner msgInner = ScheduleMessageService.this.messageTimeup(msgExt);
                PutMessageResult result = ScheduleMessageService.this.writeMessageStore.putMessage(msgInner);
                this.handleResult(result);
                if (result != null && result.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                    log.info("Resend message success, info: {}", this.toString());
                }
            } catch (Exception e) {
                this.status = ProcessStatus.EXCEPTION;
                log.error("Resend message error, info: {}", this.toString(), e);
            }
        }

        /**
         * 当重试次数大于设置的阈值（3），阻塞当前level对应的线程继续投递
         */
        public boolean need2Blocked() {
            int maxResendNum2Blocked = ScheduleMessageService.this.defaultMessageStore.getMessageStoreConfig()
                .getScheduleAsyncDeliverMaxResendNum2Blocked();
            return this.resendCount > maxResendNum2Blocked;
        }

        /**
         * 当重试的次数大于阈值（6），跳过该消息投递
         */
        public boolean need2Skip() {
            int maxResendNum2Blocked = ScheduleMessageService.this.defaultMessageStore.getMessageStoreConfig()
                .getScheduleAsyncDeliverMaxResendNum2Blocked();
            return this.resendCount > maxResendNum2Blocked * 2;
        }

        @Override
        public String toString() {
            return "PutResultProcess{" +
                "topic='" + topic + '\'' +
                ", offset=" + offset +
                ", physicOffset=" + physicOffset +
                ", physicSize=" + physicSize +
                ", delayLevel=" + delayLevel +
                ", msgId='" + msgId + '\'' +
                ", autoResend=" + autoResend +
                ", resendCount=" + resendCount +
                ", status=" + status +
                '}';
        }
    }

    public enum ProcessStatus {
        /**
         * In process, the processing result has not yet been returned.
         * */
        RUNNING,

        /**
         * Put message success.
         * */
        SUCCESS,

        /**
         * Put message exception.
         * When autoResend is true, the message will be resend.
         * */
        EXCEPTION,

        /**
         * Skip put message.
         * When the message cannot be looked, the message will be skipped.
         * */
        SKIP,
    }
}
