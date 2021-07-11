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
package com.ctrip.framework.apollo.biz.entity;

import com.google.common.base.MoreObjects;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.persistence.Table;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@Entity
@Table(name = "InstanceConfig")
public class InstanceConfig {

  /**
   * 编号
   */
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "Id")
  private long id;

  /**
   * Instance 编号，指向 {@link Instance#id}
   */
  @Column(name = "InstanceId")
  private long instanceId;

  /**
   * App 编号
   */
  @Column(name = "ConfigAppId", nullable = false)
  private String configAppId;

  /**
   * Cluster 名字
   */
  @Column(name = "ConfigClusterName", nullable = false)
  private String configClusterName;

  /**
   * Namespace 名字
   */
  @Column(name = "ConfigNamespaceName", nullable = false)
  private String configNamespaceName;

  // 通过 releaseKey + releaseDeliveryTime 字段，可以很容易判断 Instance 在当前 Namespace 获取配置的情况。

  /**
   * Release Key ，对应 {@link Release#releaseKey}
   */
  @Column(name = "ReleaseKey", nullable = false)
  private String releaseKey;

  /**
   * 配置下发时间
   */
  @Column(name = "ReleaseDeliveryTime", nullable = false)
  private Date releaseDeliveryTime;

  /**
   * 数据创建时间
   */
  @Column(name = "DataChange_CreatedTime", nullable = false)
  private Date dataChangeCreatedTime;

  /**
   * 数据最后更新时间
   */
  @Column(name = "DataChange_LastTime")
  private Date dataChangeLastModifiedTime;

  @PrePersist
  protected void prePersist() {
    if (this.dataChangeCreatedTime == null) {
      dataChangeCreatedTime = new Date();
    }
    if (this.dataChangeLastModifiedTime == null) {
      dataChangeLastModifiedTime = dataChangeCreatedTime;
    }
  }

  @PreUpdate
  protected void preUpdate() {
    this.dataChangeLastModifiedTime = new Date();
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public long getInstanceId() {
    return instanceId;
  }

  public void setInstanceId(long instanceId) {
    this.instanceId = instanceId;
  }

  public String getConfigAppId() {
    return configAppId;
  }

  public void setConfigAppId(String configAppId) {
    this.configAppId = configAppId;
  }

  public String getConfigNamespaceName() {
    return configNamespaceName;
  }

  public void setConfigNamespaceName(String configNamespaceName) {
    this.configNamespaceName = configNamespaceName;
  }

  public String getReleaseKey() {
    return releaseKey;
  }

  public void setReleaseKey(String releaseKey) {
    this.releaseKey = releaseKey;
  }

  public Date getDataChangeCreatedTime() {
    return dataChangeCreatedTime;
  }

  public void setDataChangeCreatedTime(Date dataChangeCreatedTime) {
    this.dataChangeCreatedTime = dataChangeCreatedTime;
  }

  public Date getDataChangeLastModifiedTime() {
    return dataChangeLastModifiedTime;
  }

  public void setDataChangeLastModifiedTime(Date dataChangeLastModifiedTime) {
    this.dataChangeLastModifiedTime = dataChangeLastModifiedTime;
  }

  public String getConfigClusterName() {
    return configClusterName;
  }

  public void setConfigClusterName(String configClusterName) {
    this.configClusterName = configClusterName;
  }

  public Date getReleaseDeliveryTime() {
    return releaseDeliveryTime;
  }

  public void setReleaseDeliveryTime(Date releaseDeliveryTime) {
    this.releaseDeliveryTime = releaseDeliveryTime;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .omitNullValues()
        .add("id", id)
        .add("configAppId", configAppId)
        .add("configClusterName", configClusterName)
        .add("configNamespaceName", configNamespaceName)
        .add("releaseKey", releaseKey)
        .add("dataChangeCreatedTime", dataChangeCreatedTime)
        .add("dataChangeLastModifiedTime", dataChangeLastModifiedTime)
        .toString();
  }
}
