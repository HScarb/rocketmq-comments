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
package org.apache.rocketmq.acl.plain;

import org.apache.rocketmq.acl.AccessResource;
import org.apache.rocketmq.acl.AccessValidator;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.acl.common.Permission;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.common.AclConfig;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.common.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.apache.rocketmq.acl.plain.PlainAccessResource.getRetryTopic;

public class PlainAccessValidator implements AccessValidator {

    private PlainPermissionManager aclPlugEngine;

    public PlainAccessValidator() {
        // PlainPermissionManager 用作解析 plain_acl.yml 文件
        aclPlugEngine = new PlainPermissionManager();
    }

    /**
     * 从请求头中解析本次请求对应的访问权限，构建 AccessResource 对象，为后续的权限校验做准备
     *
     * @param request 请求对象
     * @param remoteAddr
     * @return
     */
    @Override
    public AccessResource parse(RemotingCommand request, String remoteAddr) {
        // 创建 PlainAccessResource 对象，用来表示一次请求需要访问的权限
        PlainAccessResource accessResource = new PlainAccessResource();
        // 从远程地址中提取远程访问的 IP 地址
        if (remoteAddr != null && remoteAddr.contains(":")) {
            accessResource.setWhiteRemoteAddress(remoteAddr.substring(0, remoteAddr.lastIndexOf(':')));
        } else {
            accessResource.setWhiteRemoteAddress(remoteAddr);
        }

        accessResource.setRequestCode(request.getCode());

        // 如果请求中的扩展字段为空，直接返回资源，后续的访问控制只针对 IP 地址
        if (request.getExtFields() == null) {
            // If request's extFields is null,then return accessResource directly(users can use whiteAddress pattern)
            // The following logic codes depend on the request's extFields not to be null.
            return accessResource;
        }
        // 从请求体中提取客户端的访问用户名、前面字符串、安全令牌
        accessResource.setAccessKey(request.getExtFields().get(SessionCredentials.ACCESS_KEY));
        accessResource.setSignature(request.getExtFields().get(SessionCredentials.SIGNATURE));
        accessResource.setSecretToken(request.getExtFields().get(SessionCredentials.SECURITY_TOKEN));

        // 根据请求命令设置本次请求需要获得的权限
        try {
            switch (request.getCode()) {
                case RequestCode.SEND_MESSAGE:
                    final String topic = request.getExtFields().get("topic");
                    if (PlainAccessResource.isRetryTopic(topic)) {
                        accessResource.addResourceAndPerm(getRetryTopic(request.getExtFields().get("group")), Permission.SUB);
                    } else {
                        accessResource.addResourceAndPerm(topic, Permission.PUB);
                    }
                    break;
                case RequestCode.SEND_MESSAGE_V2:
                    final String topicV2 = request.getExtFields().get("b");
                    if (PlainAccessResource.isRetryTopic(topicV2)) {
                        accessResource.addResourceAndPerm(getRetryTopic(request.getExtFields().get("a")), Permission.SUB);
                    } else {
                        accessResource.addResourceAndPerm(topicV2, Permission.PUB);
                    }
                    break;
                case RequestCode.CONSUMER_SEND_MSG_BACK:
                    accessResource.addResourceAndPerm(getRetryTopic(request.getExtFields().get("group")), Permission.SUB);
                    break;
                case RequestCode.PULL_MESSAGE:
                    accessResource.addResourceAndPerm(request.getExtFields().get("topic"), Permission.SUB);
                    accessResource.addResourceAndPerm(getRetryTopic(request.getExtFields().get("consumerGroup")), Permission.SUB);
                    break;
                case RequestCode.QUERY_MESSAGE:
                    accessResource.addResourceAndPerm(request.getExtFields().get("topic"), Permission.SUB);
                    break;
                case RequestCode.HEART_BEAT:
                    HeartbeatData heartbeatData = HeartbeatData.decode(request.getBody(), HeartbeatData.class);
                    for (ConsumerData data : heartbeatData.getConsumerDataSet()) {
                        accessResource.addResourceAndPerm(getRetryTopic(data.getGroupName()), Permission.SUB);
                        for (SubscriptionData subscriptionData : data.getSubscriptionDataSet()) {
                            accessResource.addResourceAndPerm(subscriptionData.getTopic(), Permission.SUB);
                        }
                    }
                    break;
                case RequestCode.UNREGISTER_CLIENT:
                    final UnregisterClientRequestHeader unregisterClientRequestHeader =
                        (UnregisterClientRequestHeader) request
                            .decodeCommandCustomHeader(UnregisterClientRequestHeader.class);
                    accessResource.addResourceAndPerm(getRetryTopic(unregisterClientRequestHeader.getConsumerGroup()), Permission.SUB);
                    break;
                case RequestCode.GET_CONSUMER_LIST_BY_GROUP:
                    final GetConsumerListByGroupRequestHeader getConsumerListByGroupRequestHeader =
                        (GetConsumerListByGroupRequestHeader) request
                            .decodeCommandCustomHeader(GetConsumerListByGroupRequestHeader.class);
                    accessResource.addResourceAndPerm(getRetryTopic(getConsumerListByGroupRequestHeader.getConsumerGroup()), Permission.SUB);
                    break;
                case RequestCode.UPDATE_CONSUMER_OFFSET:
                    final UpdateConsumerOffsetRequestHeader updateConsumerOffsetRequestHeader =
                        (UpdateConsumerOffsetRequestHeader) request
                            .decodeCommandCustomHeader(UpdateConsumerOffsetRequestHeader.class);
                    accessResource.addResourceAndPerm(getRetryTopic(updateConsumerOffsetRequestHeader.getConsumerGroup()), Permission.SUB);
                    accessResource.addResourceAndPerm(updateConsumerOffsetRequestHeader.getTopic(), Permission.SUB);
                    break;
                default:
                    break;

            }
        } catch (Throwable t) {
            throw new AclException(t.getMessage(), t);
        }

        // 对扩展字段进行排序（SortedMap），以便生成签名字符串
        // Content
        SortedMap<String, String> map = new TreeMap<String, String>();
        for (Map.Entry<String, String> entry : request.getExtFields().entrySet()) {
            if (!SessionCredentials.SIGNATURE.equals(entry.getKey())
                // 4.9.3 以及更早的版本之前，客户端计算签名时不会将 UNIQUE_MSG_QUERY_FLAG 字段计算在内，所以 Broker 端也不需要参与签名
                && !MixAll.UNIQUE_MSG_QUERY_FLAG.equals(entry.getKey())) {
                map.put(entry.getKey(), entry.getValue());
            }
        }
        // 将扩展字段和请求体写入 content 字段
        accessResource.setContent(AclUtils.combineRequestContent(request, map));
        return accessResource;
    }

    @Override
    public void validate(AccessResource accessResource) {
        aclPlugEngine.validate((PlainAccessResource) accessResource);
    }

    @Override
    public boolean updateAccessConfig(PlainAccessConfig plainAccessConfig) {
        return aclPlugEngine.updateAccessConfig(plainAccessConfig);
    }

    @Override
    public boolean deleteAccessConfig(String accesskey) {
        return aclPlugEngine.deleteAccessConfig(accesskey);
    }

    @Override public String  getAclConfigVersion() {
        return aclPlugEngine.getAclConfigDataVersion();
    }

    @Override public boolean updateGlobalWhiteAddrsConfig(List<String> globalWhiteAddrsList) {
        return aclPlugEngine.updateGlobalWhiteAddrsConfig(globalWhiteAddrsList);
    }

    @Override public boolean updateGlobalWhiteAddrsConfig(List<String> globalWhiteAddrsList, String aclFileFullPath) {
        return aclPlugEngine.updateGlobalWhiteAddrsConfig(globalWhiteAddrsList, aclFileFullPath);
    }

    @Override public AclConfig getAllAclConfig() {
        return aclPlugEngine.getAllAclConfig();
    }
    
    @Override
    public Map<String, DataVersion> getAllAclConfigVersion() {
        return aclPlugEngine.getDataVersionMap();
    }
}
