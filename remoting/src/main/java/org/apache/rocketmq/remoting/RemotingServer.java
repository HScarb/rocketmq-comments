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
package org.apache.rocketmq.remoting;

import io.netty.channel.Channel;
import java.util.concurrent.ExecutorService;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;

/**
 * 处理远程通信的 Server 接口
 */
public interface RemotingServer extends RemotingService {

    /**
     * 注册请求处理器，例如消息发送、消息拉取等每一个网络操作会定义一个请求编码（requestCode），
     * 然后每一个类型对应一个业务处理器 NettyRequestProcessor，
     * 并可以按照不同的 requestCode 定义不同的线程池，实现不同请求的线程池隔离
     *
     * @param requestCode 请求码，RocketMQ 按照业务对请求进行拆分，详见 {@link RequestCode}
     * @param processor 该 requestCode 对应的业务处理器
     * @param executor 该 requestCode 对应的的处理线程池，必须要对应一个队列大小有限制的阻塞队列，防止 OOM
     */
    void registerProcessor(final int requestCode, final NettyRequestProcessor processor,
        final ExecutorService executor);

    /**
     * 注册所有请求默认的处理器，当请求的 requestCode 未注册时，会使用默认的处理器进行处理
     * Broker 默认的处理器为 AdminBrokerProcessor，用于处理发送到 Broker 的管理请求
     */
    void registerDefaultProcessor(final NettyRequestProcessor processor, final ExecutorService executor);

    int localListenPort();

    /**
     * 根据请求码获取对应的请求处理器与线程池
     */
    Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(final int requestCode);

    /**
     * 获取默认的请求处理器
     */
    Pair<NettyRequestProcessor, ExecutorService> getDefaultProcessorPair();

    RemotingServer newRemotingServer(int port);

    void removeRemotingServer(int port);

    /**
     * 服务端同步发送请求（这里用于向客户端发送请求）
     * @param channel Netty 通道
     * @param request RPC 请求对象
     * @param timeoutMillis 超时
     * @return
     */
    RemotingCommand invokeSync(final Channel channel, final RemotingCommand request,
        final long timeoutMillis) throws InterruptedException, RemotingSendRequestException,
        RemotingTimeoutException;

    /**
     * 服务端异步发送请求
     */
    void invokeAsync(final Channel channel, final RemotingCommand request, final long timeoutMillis,
        final InvokeCallback invokeCallback) throws InterruptedException,
        RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    /**
     * 服务端单向请求调用（不等待响应）
     */
    void invokeOneway(final Channel channel, final RemotingCommand request, final long timeoutMillis)
        throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException,
        RemotingSendRequestException;

}
