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
package org.apache.rocketmq.common;

public class KeyBuilder {
    public static final int POP_ORDER_REVIVE_QUEUE = 999;

    /**
     * POP 消费重试 Topic
     * %RETRY%{客户端ID}_{TOPIC}
     *
     * @param topic
     * @param cid
     * @return
     */
    public static String buildPopRetryTopic(String topic, String cid) {
        return MixAll.RETRY_GROUP_TOPIC_PREFIX + cid + "_" + topic;
    }

    public static String parseNormalTopic(String topic, String cid) {
        if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            return topic.substring((MixAll.RETRY_GROUP_TOPIC_PREFIX + cid + "_").length());
        } else {
            return topic;
        }
    }

    /**
     * Pop 消费长轮询 Key
     * TOPIC@GROUP@QUEUE_ID
     *
     * @param topic
     * @param cid
     * @param queueId
     * @return
     */
    public static String buildPollingKey(String topic, String cid, int queueId) {
        return topic + PopAckConstants.SPLIT + cid + PopAckConstants.SPLIT + queueId;
    }

    public static String buildPollingNotificationKey(String topic, int queueId) {
        return topic + PopAckConstants.SPLIT + queueId;
    }
}
