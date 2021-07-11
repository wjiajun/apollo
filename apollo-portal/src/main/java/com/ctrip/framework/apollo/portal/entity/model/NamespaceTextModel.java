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
package com.ctrip.framework.apollo.portal.entity.model;


import com.ctrip.framework.apollo.core.enums.ConfigFileFormat;
import com.ctrip.framework.apollo.portal.environment.Env;
import com.ctrip.framework.apollo.core.utils.StringUtils;

public class NamespaceTextModel implements Verifiable {

  /**
   * App 编号
   */
  private String appId;

  /**
   * Env 名
   */
  private String env;

  /**
   * Cluster 名
   */
  private String clusterName;

  /**
   * Namespace 名
   */
  private String namespaceName;

  /**
   * Namespace 编号
   */
  private long namespaceId;

  /**
   * 格式
   */
  private String format;

  /**
   * 配置文本
   */
  private String configText;


  @Override
  public boolean isInvalid() {
    return StringUtils.isContainEmpty(appId, env, clusterName, namespaceName) || namespaceId <= 0;
  }

  public String getAppId() {
    return appId;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public Env getEnv() {
    return Env.valueOf(env);
  }

  public void setEnv(String env) {
    this.env = env;
  }

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public String getNamespaceName() {
    return namespaceName;
  }

  public void setNamespaceName(String namespaceName) {
    this.namespaceName = namespaceName;
  }

  public long getNamespaceId() {
    return namespaceId;
  }

  public void setNamespaceId(long namespaceId) {
    this.namespaceId = namespaceId;
  }

  public String getConfigText() {
    return configText;
  }

  public void setConfigText(String configText) {
    this.configText = configText;
  }

  public ConfigFileFormat getFormat() {
    return ConfigFileFormat.fromString(this.format);
  }

  public void setFormat(String format) {
    this.format = format;
  }
}
