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


import com.ctrip.framework.apollo.portal.environment.Env;
import com.ctrip.framework.apollo.core.utils.StringUtils;

public class NamespaceReleaseModel implements Verifiable {

  /**
   * App 编号
   */
  private String appId;
  /**
   * Env 名字
   */
  private String env;
  /**
   * Cluster 名字
   */
  private String clusterName;
  /**
   * Namespace 名字
   */
  private String namespaceName;
  /**
   * 发布标题
   */
  private String releaseTitle;
  /**
   * 发布描述
   */
  private String releaseComment;
  /**
   * 发布人
   */
  private String releasedBy;
  /**
   * 是否紧急发布
   */
  private boolean isEmergencyPublish;


  @Override
  public boolean isInvalid() {
    return StringUtils.isContainEmpty(appId, env, clusterName, namespaceName, releaseTitle);
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

  public String getReleaseTitle() {
    return releaseTitle;
  }

  public void setReleaseTitle(String releaseTitle) {
    this.releaseTitle = releaseTitle;
  }

  public String getReleaseComment() {
    return releaseComment;
  }

  public void setReleaseComment(String releaseComment) {
    this.releaseComment = releaseComment;
  }

  public String getReleasedBy() {
    return releasedBy;
  }

  public void setReleasedBy(String releasedBy) {
    this.releasedBy = releasedBy;
  }

  public boolean isEmergencyPublish() {
    return isEmergencyPublish;
  }

  public void setEmergencyPublish(boolean emergencyPublish) {
    isEmergencyPublish = emergencyPublish;
  }
}
