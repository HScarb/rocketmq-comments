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
package org.apache.rocketmq.broker.dledger;

import io.openmessaging.storage.dledger.DLedgerLeaderElector;
import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.MemberState;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.dledger.DLedgerCommitLog;

/**
 * DLedger 节点角色变化监听器
 */
public class DLedgerRoleChangeHandler implements DLedgerLeaderElector.RoleChangeHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private ExecutorService executorService;
    private BrokerController brokerController;
    private DefaultMessageStore messageStore;
    private DLedgerCommitLog dLedgerCommitLog;
    private DLedgerServer dLegerServer;
    private Future<?> slaveSyncFuture;
    private long lastSyncTimeMs = System.currentTimeMillis();

    public DLedgerRoleChangeHandler(BrokerController brokerController, DefaultMessageStore messageStore) {
        this.brokerController = brokerController;
        this.messageStore = messageStore;
        this.dLedgerCommitLog = (DLedgerCommitLog) messageStore.getCommitLog();
        this.dLegerServer = dLedgerCommitLog.getdLedgerServer();
        this.executorService = Executors.newSingleThreadExecutor(
            new ThreadFactoryImpl("DLegerRoleChangeHandler_", brokerController.getBrokerIdentity()));
    }

    @Override
    public void handle(long term, MemberState.Role role) {
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                long start = System.currentTimeMillis();
                try {
                    boolean succ = true;
                    LOGGER.info("Begin handling broker role change term={} role={} currStoreRole={}", term, role, messageStore.getMessageStoreConfig().getBrokerRole());
                    switch (role) {
                        case CANDIDATE:
                            // 角色切换为候选人，说明当前在选主截断。如果当前节点不是 Follower，切换成 Follower
                            if (messageStore.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE) {
                                changeToSlave(dLedgerCommitLog.getId());
                            }
                            break;
                        case FOLLOWER:
                            changeToSlave(dLedgerCommitLog.getId());
                            break;
                        case LEADER:
                            // 切换成 Leader，在切换之前，需要等待当前节点追加的数据都提交之后才能变更状态
                            while (true) {
                                if (!dLegerServer.getMemberState().isLeader()) {
                                    succ = false;
                                    break;
                                }
                                // ledgerEndIndex 为 -1，表示当前节点还没有数据转发，直接跳出
                                if (dLegerServer.getdLedgerStore().getLedgerEndIndex() == -1) {
                                    break;
                                }
                                // 等待数据全部提交，即 ledgerEndIndex == committedIndex
                                // 且 ConsumeQueue 文件构建完毕，即 dispatchBehindBytes == 0
                                if (dLegerServer.getdLedgerStore().getLedgerEndIndex() == dLegerServer.getdLedgerStore().getCommittedIndex()
                                    && messageStore.dispatchBehindBytes() == 0) {
                                    break;
                                }
                                // 数据未全部提交则继续等待
                                Thread.sleep(100);
                            }
                            if (succ) {
                                messageStore.recoverTopicQueueTable();
                                changeToMaster(BrokerRole.SYNC_MASTER);
                            }
                            break;
                        default:
                            break;
                    }
                    LOGGER.info("Finish handling broker role change succ={} term={} role={} currStoreRole={} cost={}", succ, term, role, messageStore.getMessageStoreConfig().getBrokerRole(), DLedgerUtils.elapsed(start));
                } catch (Throwable t) {
                    LOGGER.info("[MONITOR]Failed handling broker role change term={} role={} currStoreRole={} cost={}", term, role, messageStore.getMessageStoreConfig().getBrokerRole(), DLedgerUtils.elapsed(start), t);
                }
            }
        };
        executorService.submit(runnable);
    }

    /**
     * 切换成 Follower 时，开启元数据同步
     * @param role
     */
    private void handleSlaveSynchronize(BrokerRole role) {
        if (role == BrokerRole.SLAVE) {
            if (null != slaveSyncFuture) {
                // 上次同步的 future 不为空，先取消
                slaveSyncFuture.cancel(false);
            }
            // 设置主从同步的主节点地址为空，该值从 Broker 向 NameServer 发送的心跳包响应结果中获取，每 10s 一次
            this.brokerController.getSlaveSynchronize().setMasterAddr(null);
            // 开启元数据同步定时任务，每 3s 同步一次
            slaveSyncFuture = this.brokerController.getScheduledExecutorService().scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (System.currentTimeMillis() - lastSyncTimeMs > 10 * 1000) {
                            // 同步所有元数据
                            brokerController.getSlaveSynchronize().syncAll();
                            lastSyncTimeMs = System.currentTimeMillis();
                        }
                        // 同步定时消息 check point
                        //timer checkpoint, latency-sensitive, so sync it more frequently
                        brokerController.getSlaveSynchronize().syncTimerCheckPoint();
                    } catch (Throwable e) {
                        LOGGER.error("ScheduledTask SlaveSynchronize syncAll error.", e);
                    }
                }
            }, 1000 * 3, 1000 * 3, TimeUnit.MILLISECONDS);
        } else {
            // 如果当前节点为 Leader，取消定时同步任务
            //handle the slave synchronise
            if (null != slaveSyncFuture) {
                slaveSyncFuture.cancel(false);
            }
            // 设置主节点地址为空
            this.brokerController.getSlaveSynchronize().setMasterAddr(null);
        }
    }

    /**
     * Broker 角色切换为 Follower
     *
     * @param brokerId
     */
    public void changeToSlave(int brokerId) {
        LOGGER.info("Begin to change to slave brokerName={} brokerId={}", this.brokerController.getBrokerConfig().getBrokerName(), brokerId);

        // 如果 Broker ID 为 0，需要设为 1，因为 0 被 Leader 使用
        //change the role
        this.brokerController.getBrokerConfig().setBrokerId(brokerId == 0 ? 1 : brokerId); //TO DO check
        this.brokerController.getMessageStoreConfig().setBrokerRole(BrokerRole.SLAVE);

        // 关闭延迟消息服务和事务消息回查服务
        this.brokerController.changeSpecialServiceStatus(false);

        // 启动主从元数据同步处理器
        //handle the slave synchronise
        handleSlaveSynchronize(BrokerRole.SLAVE);

        try {
            // 立即向集群内所有 NameServer 发送心跳，告知 Broker 状态变更
            this.brokerController.registerBrokerAll(true, true, this.brokerController.getBrokerConfig().isForceRegister());
        } catch (Throwable ignored) {

        }
        LOGGER.info("Finish to change to slave brokerName={} brokerId={}", this.brokerController.getBrokerConfig().getBrokerName(), brokerId);
    }

    /**
     * Broker 角色切换为 Leader
     * @param role
     */
    public void changeToMaster(BrokerRole role) {
        if (role == BrokerRole.SLAVE) {
            return;
        }
        LOGGER.info("Begin to change to master brokerName={}", this.brokerController.getBrokerConfig().getBrokerName());

        // 取消主从同步处理器
        //handle the slave synchronise
        handleSlaveSynchronize(role);

        // 启动延迟消息服务和事务消息回查服务
        this.brokerController.changeSpecialServiceStatus(true);

        // 设置 Broker ID 为 0，表示 Leader 节点
        //if the operations above are totally successful, we change to master
        this.brokerController.getBrokerConfig().setBrokerId(0); //TO DO check
        this.brokerController.getMessageStoreConfig().setBrokerRole(role);

        try {
            // 立即向集群中所有 NameServer 发送心跳，告知 Broker 状态变更
            this.brokerController.registerBrokerAll(true, true, this.brokerController.getBrokerConfig().isForceRegister());
        } catch (Throwable ignored) {

        }
        LOGGER.info("Finish to change to master brokerName={}", this.brokerController.getBrokerConfig().getBrokerName());
    }

    @Override
    public void startup() {

    }

    @Override
    public void shutdown() {
        executorService.shutdown();
    }
}
