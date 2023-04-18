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

import java.util.Map;
import org.apache.rocketmq.acl.AccessResource;
import org.apache.rocketmq.acl.PermissionChecker;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.Permission;

public class PlainPermissionChecker implements PermissionChecker {
    /**
     * 验证是否有某资源的访问权限
     *
     * @param checkedAccess 客户端请求需要的资源权限
     * @param ownedAccess 该账号拥有的资源权限
     */
    public void check(AccessResource checkedAccess, AccessResource ownedAccess) {
        PlainAccessResource checkedPlainAccess = (PlainAccessResource) checkedAccess;
        PlainAccessResource ownedPlainAccess = (PlainAccessResource) ownedAccess;
        if (Permission.needAdminPerm(checkedPlainAccess.getRequestCode()) && !ownedPlainAccess.isAdmin()) {
            throw new AclException(String.format("Need admin permission for request code=%d, but accessKey=%s is not", checkedPlainAccess.getRequestCode(), ownedPlainAccess.getAccessKey()));
        }
        Map<String, Byte> needCheckedPermMap = checkedPlainAccess.getResourcePermMap();
        Map<String, Byte> ownedPermMap = ownedPlainAccess.getResourcePermMap();

        if (needCheckedPermMap == null) {
            // 本次操作无需权限验证，直接通过
            // If the needCheckedPermMap is null,then return
            return;
        }

        if (ownedPermMap == null && ownedPlainAccess.isAdmin()) {
            // 该账号未设置任何访问规则，且用户是管理员，直接通过
            // If the ownedPermMap is null and it is an admin user, then return
            return;
        }

        // 遍历需要的权限与拥有的权限进行对比
        for (Map.Entry<String, Byte> needCheckedEntry : needCheckedPermMap.entrySet()) {
            String resource = needCheckedEntry.getKey();
            Byte neededPerm = needCheckedEntry.getValue();
            // 重试 Topic 需要获取默认消费组的权限
            boolean isGroup = PlainAccessResource.isRetryTopic(resource);

            if (ownedPermMap == null || !ownedPermMap.containsKey(resource)) {
                // 对于重试 Topic，它不会被配置在 ACL 文件中，获取默认消费组的权限
                // Check the default perm
                byte ownedPerm = isGroup ? ownedPlainAccess.getDefaultGroupPerm() :
                    ownedPlainAccess.getDefaultTopicPerm();
                if (!Permission.checkPermission(neededPerm, ownedPerm)) {
                    throw new AclException(String.format("No default permission for %s", PlainAccessResource.printStr(resource, isGroup)));
                }
                continue;
            }
            if (!Permission.checkPermission(neededPerm, ownedPermMap.get(resource))) {
                throw new AclException(String.format("No default permission for %s", PlainAccessResource.printStr(resource, isGroup)));
            }
        }
    }
}
