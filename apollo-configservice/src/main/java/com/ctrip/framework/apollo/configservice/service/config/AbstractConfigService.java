/*
 * Copyright 2021 Apollo Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.ctrip.framework.apollo.configservice.service.config;

import com.ctrip.framework.apollo.biz.entity.Release;
import com.ctrip.framework.apollo.biz.grayReleaseRule.GrayReleaseRulesHolder;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.apollo.core.dto.ApolloNotificationMessages;

import com.google.common.base.Strings;

import java.util.Objects;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author Jason Song(song_s@ctrip.com)
 * 实现公用的获取配置的逻辑，并暴露抽象方法，让子类实现
 */
public abstract class AbstractConfigService implements ConfigService {
  @Autowired
  private GrayReleaseRulesHolder grayReleaseRulesHolder;

  @Override
  public Release loadConfig(String clientAppId, String clientIp, String configAppId, String configClusterName,
      String configNamespace, String dataCenter, ApolloNotificationMessages clientMessages) {
    // load from specified cluster first
    // 优先，获得指定 Cluster 的 Release 。若存在，直接返回。
    if (!Objects.equals(ConfigConsts.CLUSTER_NAME_DEFAULT, configClusterName)) {
      Release clusterRelease = findRelease(clientAppId, clientIp, configAppId, configClusterName, configNamespace,
          clientMessages);

      if (Objects.nonNull(clusterRelease)) {
        return clusterRelease;
      }
    }

    // 其次，获得所属 IDC 的 Cluster 的 Release 。若存在，直接返回
    // try to load via data center
    if (!Strings.isNullOrEmpty(dataCenter) && !Objects.equals(dataCenter, configClusterName)) {
      Release dataCenterRelease = findRelease(clientAppId, clientIp, configAppId, dataCenter, configNamespace,
          clientMessages);
      if (Objects.nonNull(dataCenterRelease)) {
        return dataCenterRelease;
      }
    }

    // 最后，获得默认 Cluster 的 Release 。
    // fallback to default release
    return findRelease(clientAppId, clientIp, configAppId, ConfigConsts.CLUSTER_NAME_DEFAULT, configNamespace,
        clientMessages);
  }

  /**
   * Find release
   *
   * 获得 Release 对象
   * 
   * @param clientAppId the client's app id
   * @param clientIp the client ip
   * @param configAppId the requested config's app id
   * @param configClusterName the requested config's cluster name
   * @param configNamespace the requested config's namespace name
   * @param clientMessages the messages received in client side
   * @return the release
   */
  private Release findRelease(String clientAppId, String clientIp, String configAppId, String configClusterName,
      String configNamespace, ApolloNotificationMessages clientMessages) {
    // 读取灰度发布编号
    Long grayReleaseId = grayReleaseRulesHolder.findReleaseIdFromGrayReleaseRule(clientAppId, clientIp, configAppId,
        configClusterName, configNamespace);

    //  读取灰度 Release 对象
    Release release = null;
    if (grayReleaseId != null) {
      release = findActiveOne(grayReleaseId, clientMessages);
    }

    // 非灰度，获得最新的，并且有效的 Release 对象
    if (release == null) {
      release = findLatestActiveRelease(configAppId, configClusterName, configNamespace, clientMessages);
    }

    return release;
  }

  /**
   * 获得指定编号，并且有效的 Release 对象
   *
   * Find active release by id
   *
   * @param id Release 编号
   */
  protected abstract Release findActiveOne(long id, ApolloNotificationMessages clientMessages);

  /**
   * 获得最新的，并且有效的 Release 对象
   *
   * Find active release by app id, cluster name and namespace name
   */
  protected abstract Release findLatestActiveRelease(String configAppId, String configClusterName,
      String configNamespaceName, ApolloNotificationMessages clientMessages);
}
