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
package com.ctrip.framework.apollo.internals;

import com.ctrip.framework.apollo.core.utils.DeferredLoggerFactory;
import com.ctrip.framework.apollo.enums.ConfigSourceType;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;

import com.ctrip.framework.apollo.core.utils.ClassLoaderUtil;
import com.ctrip.framework.apollo.enums.PropertyChangeType;
import com.ctrip.framework.apollo.model.ConfigChange;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.util.ExceptionUtil;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.RateLimiter;


/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class DefaultConfig extends AbstractConfig implements RepositoryChangeListener {

  private static final Logger logger = DeferredLoggerFactory.getLogger(DefaultConfig.class);
  /**
   * Namespace 的名字
   */
  private final String m_namespace;
  /**
   * 项目下，Namespace 对应的配置文件的 Properties
   */
  private final Properties m_resourceProperties;
  /**
   * 配置 Properties 的缓存引用
   */
  private final AtomicReference<Properties> m_configProperties;
  /**
   * 配置 Repository
   */
  private final ConfigRepository m_configRepository;
  /**
   * 打印告警限流器。当读取不到属性值，会打印告警日志。通过该限流器，避免打印过多日志。
   */
  private final RateLimiter m_warnLogRateLimiter;

  private volatile ConfigSourceType m_sourceType = ConfigSourceType.NONE;

  /**
   * Constructor.
   *
   * @param namespace        the namespace of this config instance
   * @param configRepository the config repository for this config instance
   */
  public DefaultConfig(String namespace, ConfigRepository configRepository) {
    m_namespace = namespace;
    m_resourceProperties = loadFromResource(m_namespace);
    m_configRepository = configRepository;
    m_configProperties = new AtomicReference<>();
    m_warnLogRateLimiter = RateLimiter.create(0.017); // 1 warning log output per minute
    // 初始化
    initialize();
  }

  private void initialize() {
    try {
      // 初始化 m_configProperties
      updateConfig(m_configRepository.getConfig(), m_configRepository.getSourceType());
    } catch (Throwable ex) {
      Tracer.logError(ex);
      logger.warn("Init Apollo Local Config failed - namespace: {}, reason: {}.",
          m_namespace, ExceptionUtil.getDetailMessage(ex));
    } finally {
      //register the change listener no matter config repository is working or not
      //so that whenever config repository is recovered, config could get changed
      // 注册到 ConfigRepository 中，从而实现每次配置发生变更时，更新配置缓存 `m_configProperties` 。
      m_configRepository.addChangeListener(this);
    }
  }

  @Override
  public String getProperty(String key, String defaultValue) {
    // step 1: check system properties, i.e. -Dkey=value 从系统 Properties 获得属性，例如，JVM 启动参数。
    String value = System.getProperty(key);

    // step 2: check local cached properties file 从缓存 Properties 获得属性
    if (value == null && m_configProperties.get() != null) {
      value = m_configProperties.get().getProperty(key);
    }

    /**
     * step 3: check env variable, i.e. PATH=...从环境变量中获得参数
     * normally system environment variables are in UPPERCASE, however there might be exceptions.
     * so the caller should provide the key in the right case
     */
    if (value == null) {
      value = System.getenv(key);
    }

    // step 4: check properties file from classpath
    if (value == null && m_resourceProperties != null) {
      value = m_resourceProperties.getProperty(key);
    }

    // 打印告警日志
    if (value == null && m_configProperties.get() == null && m_warnLogRateLimiter.tryAcquire()) {
      logger.warn(
          "Could not load config for namespace {} from Apollo, please check whether the configs are released in Apollo! Return default value now!",
          m_namespace);
    }

    // 若为空，使用默认值
    return value == null ? defaultValue : value;
  }

  @Override
  public Set<String> getPropertyNames() {
    Properties properties = m_configProperties.get();
    // 若为空，返回空集合
    if (properties == null) {
      return Collections.emptySet();
    }

    return stringPropertyNames(properties);
  }

  @Override
  public ConfigSourceType getSourceType() {
    return m_sourceType;
  }

  private Set<String> stringPropertyNames(Properties properties) {
    //jdk9以下版本Properties#enumerateStringProperties方法存在性能问题，keys() + get(k) 重复迭代, jdk9之后改为entrySet遍历.
    Map<String, String> h = new LinkedHashMap<>();
    for (Map.Entry<Object, Object> e : properties.entrySet()) {
      Object k = e.getKey();
      Object v = e.getValue();
      if (k instanceof String && v instanceof String) {
        h.put((String) k, (String) v);
      }
    }
    return h.keySet();
  }

  // RemoteConfigRepository 会在配置变更时，从 Admin Service 重新加载配置,从而进行回调
  @Override
  public synchronized void onRepositoryChange(String namespace, Properties newProperties) {
    // 忽略，若未变更
    if (newProperties.equals(m_configProperties.get())) {
      return;
    }

    ConfigSourceType sourceType = m_configRepository.getSourceType();
    // 读取新的 Properties 对象
    Properties newConfigProperties = propertiesFactory.getPropertiesInstance();
    newConfigProperties.putAll(newProperties);

    // 计算配置变更集合
    Map<String, ConfigChange> actualChanges = updateAndCalcConfigChanges(newConfigProperties,
        sourceType);

    //check double checked result
    if (actualChanges.isEmpty()) {
      return;
    }

    // 通知监听器们
    this.fireConfigChange(m_namespace, actualChanges);

    Tracer.logEvent("Apollo.Client.ConfigChanges", m_namespace);
  }

  private void updateConfig(Properties newConfigProperties, ConfigSourceType sourceType) {
    m_configProperties.set(newConfigProperties);
    m_sourceType = sourceType;
  }

  // 计算配置变更集合
  private Map<String, ConfigChange> updateAndCalcConfigChanges(Properties newConfigProperties,
      ConfigSourceType sourceType) {
    // 计算配置变更集合
    List<ConfigChange> configChanges =
        calcPropertyChanges(m_namespace, m_configProperties.get(), newConfigProperties);

    // 结果
    ImmutableMap.Builder<String, ConfigChange> actualChanges =
        new ImmutableMap.Builder<>();

    /** === Double check since DefaultConfig has multiple config sources ==== **/

    //1. use getProperty to update configChanges's old value
    // 重新设置每个 ConfigChange 的【旧】值
    for (ConfigChange change : configChanges) {
      change.setOldValue(this.getProperty(change.getPropertyName(), change.getOldValue()));
    }

    //2. update m_configProperties
    // 更新到 `m_configProperties` 中
    updateConfig(newConfigProperties, sourceType);
    // 清空 Cache 缓存
    clearConfigCache();

    //3. use getProperty to update configChange's new value and calc the final changes
    for (ConfigChange change : configChanges) {
      // 重新设置每个 ConfigChange 的【新】值
      change.setNewValue(this.getProperty(change.getPropertyName(), change.getNewValue()));
      // 重新计算变化类型
      switch (change.getChangeType()) {
        case ADDED:
          // 相等，忽略
          if (Objects.equals(change.getOldValue(), change.getNewValue())) {
            break;
          }
          // 老值非空，修改为变更类型
          if (change.getOldValue() != null) {
            change.setChangeType(PropertyChangeType.MODIFIED);
          }
          // 添加过结果
          actualChanges.put(change.getPropertyName(), change);
          break;
        case MODIFIED:
          // 若不相等，说明依然是变更类型，添加到结果
          if (!Objects.equals(change.getOldValue(), change.getNewValue())) {
            actualChanges.put(change.getPropertyName(), change);
          }
          break;
        case DELETED:
          // 相等，忽略
          if (Objects.equals(change.getOldValue(), change.getNewValue())) {
            break;
          }
          // 新值非空，修改为变更类型
          if (change.getNewValue() != null) {
            change.setChangeType(PropertyChangeType.MODIFIED);
          }
          // 添加过结果
          actualChanges.put(change.getPropertyName(), change);
          break;
        default:
          //do nothing
          break;
      }
    }
    return actualChanges.build();
  }

  private Properties loadFromResource(String namespace) {
    // 生成文件名
    String name = String.format("META-INF/config/%s.properties", namespace);
    // 读取 Properties 文件
    InputStream in = ClassLoaderUtil.getLoader().getResourceAsStream(name);
    Properties properties = null;

    if (in != null) {
      properties = propertiesFactory.getPropertiesInstance();

      try {
        properties.load(in);
      } catch (IOException ex) {
        Tracer.logError(ex);
        logger.error("Load resource config for namespace {} failed", namespace, ex);
      } finally {
        try {
          in.close();
        } catch (IOException ex) {
          // ignore
        }
      }
    }

    return properties;
  }
}
