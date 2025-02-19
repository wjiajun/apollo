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
package com.ctrip.framework.apollo.configservice.service;

import com.ctrip.framework.apollo.biz.config.BizConfig;
import com.ctrip.framework.apollo.biz.repository.AppNamespaceRepository;
import com.ctrip.framework.apollo.common.entity.AppNamespace;
import com.ctrip.framework.apollo.configservice.wrapper.CaseInsensitiveMapWrapper;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.core.utils.StringUtils;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@Service
public class AppNamespaceServiceWithCache implements InitializingBean {
  private static final Logger logger = LoggerFactory.getLogger(AppNamespaceServiceWithCache.class);
  private static final Joiner STRING_JOINER = Joiner.on(ConfigConsts.CLUSTER_NAMESPACE_SEPARATOR)
      .skipNulls();
  private final AppNamespaceRepository appNamespaceRepository;
  private final BizConfig bizConfig;

  /**
   * 增量初始化周期
   */
  private int scanInterval;
  /**
   * 增量初始化周期单位
   */
  private TimeUnit scanIntervalTimeUnit;
  /**
   * 重建周期
   */
  private int rebuildInterval;
  /**
   * 重建周期单位
   */
  private TimeUnit rebuildIntervalTimeUnit;
  /**
   * 定时任务 ExecutorService
   */
  private ScheduledExecutorService scheduledExecutorService;
  /**
   * 最后扫描到的 AppNamespace 的编号
   */
  private long maxIdScanned;

  //store namespaceName -> AppNamespace
  /**
   * 公用类型的 AppNamespace 的缓存
   *
   * //store namespaceName -> AppNamespace
   */
  private CaseInsensitiveMapWrapper<AppNamespace> publicAppNamespaceCache;

  //store appId+namespaceName -> AppNamespace
  /**
   * App 下的 AppNamespace 的缓存
   *
   * store appId+namespaceName -> AppNamespace
   */
  private CaseInsensitiveMapWrapper<AppNamespace> appNamespaceCache;

  //store id -> AppNamespace
  /**
   * AppNamespace 的缓存
   *
   * //store id -> AppNamespace
   */
  private Map<Long, AppNamespace> appNamespaceIdCache;

  public AppNamespaceServiceWithCache(
      final AppNamespaceRepository appNamespaceRepository,
      final BizConfig bizConfig) {
    this.appNamespaceRepository = appNamespaceRepository;
    this.bizConfig = bizConfig;
    initialize();
  }

  private void initialize() {
    maxIdScanned = 0;
    // 创建缓存对象
    publicAppNamespaceCache = new CaseInsensitiveMapWrapper<>(Maps.newConcurrentMap());
    appNamespaceCache = new CaseInsensitiveMapWrapper<>(Maps.newConcurrentMap());
    appNamespaceIdCache = Maps.newConcurrentMap();
    // 创建 ScheduledExecutorService 对象，大小为 1 。
    scheduledExecutorService = Executors.newScheduledThreadPool(1, ApolloThreadFactory
        .create("AppNamespaceServiceWithCache", true));
  }

  /**
   * 获得 AppNamespace 对象
   *
   * @param appId App 编号
   * @param namespaceName Namespace 名字
   * @return AppNamespace
   */
  public AppNamespace findByAppIdAndNamespace(String appId, String namespaceName) {
    Preconditions.checkArgument(!StringUtils.isContainEmpty(appId, namespaceName), "appId and namespaceName must not be empty");
    return appNamespaceCache.get(STRING_JOINER.join(appId, namespaceName));
  }

  /**
   * 获得 AppNamespace 对象数组
   *
   * @param appId App 编号
   * @param namespaceNames Namespace 名字的集合
   * @return AppNamespace 数组
   */
  public List<AppNamespace> findByAppIdAndNamespaces(String appId, Set<String> namespaceNames) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(appId), "appId must not be null");
    if (namespaceNames == null || namespaceNames.isEmpty()) {
      return Collections.emptyList();
    }
    List<AppNamespace> result = Lists.newArrayList();
    for (String namespaceName : namespaceNames) {
      AppNamespace appNamespace = appNamespaceCache.get(STRING_JOINER.join(appId, namespaceName));
      if (appNamespace != null) {
        result.add(appNamespace);
      }
    }
    return result;
  }

  public AppNamespace findPublicNamespaceByName(String namespaceName) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(namespaceName), "namespaceName must not be empty");
    return publicAppNamespaceCache.get(namespaceName);
  }

  /**
   * 获得公用类型的 AppNamespace 对象数组
   *
   * @param namespaceNames Namespace 名字的集合
   * @return AppNamespace 数组
   */
  public List<AppNamespace> findPublicNamespacesByNames(Set<String> namespaceNames) {
    if (namespaceNames == null || namespaceNames.isEmpty()) {
      return Collections.emptyList();
    }

    List<AppNamespace> result = Lists.newArrayList();
    for (String namespaceName : namespaceNames) {
      AppNamespace appNamespace = publicAppNamespaceCache.get(namespaceName);
      if (appNamespace != null) {
        result.add(appNamespace);
      }
    }
    return result;
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    // 从 ServerConfig 中，读取定时任务的周期配置
    populateDataBaseInterval();
    // 全量初始化 AppNamespace 缓存
    scanNewAppNamespaces(); //block the startup process until load finished
    // 创建定时任务，全量重构 AppNamespace 缓存
    // 考虑 AppNamespace 更新与删除
    scheduledExecutorService.scheduleAtFixedRate(() -> {
      Transaction transaction = Tracer.newTransaction("Apollo.AppNamespaceServiceWithCache",
          "rebuildCache");
      try {
        // 全量重建 AppNamespace 缓存
        this.updateAndDeleteCache();
        transaction.setStatus(Transaction.SUCCESS);
      } catch (Throwable ex) {
        transaction.setStatus(ex);
        logger.error("Rebuild cache failed", ex);
      } finally {
        transaction.complete();
      }
    }, rebuildInterval, rebuildInterval, rebuildIntervalTimeUnit);
    // 创建定时任务，增量初始化 AppNamespace 缓存
    // 考虑 AppNamespace 新增
    scheduledExecutorService.scheduleWithFixedDelay(this::scanNewAppNamespaces, scanInterval,
        scanInterval, scanIntervalTimeUnit);
  }

  private void scanNewAppNamespaces() {
    Transaction transaction = Tracer.newTransaction("Apollo.AppNamespaceServiceWithCache",
        "scanNewAppNamespaces");
    try {
      // 加载新的 AppNamespace 们
      this.loadNewAppNamespaces();
      transaction.setStatus(Transaction.SUCCESS);
    } catch (Throwable ex) {
      transaction.setStatus(ex);
      logger.error("Load new app namespaces failed", ex);
    } finally {
      transaction.complete();
    }
  }

  //for those new app namespaces
  private void loadNewAppNamespaces() {
    boolean hasMore = true;
    while (hasMore && !Thread.currentThread().isInterrupted()) { // 循环，直到无新的 AppNamespace
      //current batch is 500
      // 获得大于 maxIdScanned 的 500 条 AppNamespace 记录，按照 id 升序
      List<AppNamespace> appNamespaces = appNamespaceRepository
          .findFirst500ByIdGreaterThanOrderByIdAsc(maxIdScanned);
      if (CollectionUtils.isEmpty(appNamespaces)) {
        break;
      }
      // 合并到 AppNamespace 缓存中
      mergeAppNamespaces(appNamespaces);
      // 获得新的 maxIdScanned ，取最后一条记录
      int scanned = appNamespaces.size();
      maxIdScanned = appNamespaces.get(scanned - 1).getId();
      // 若拉取不足 500 条，说明无新消息了
      hasMore = scanned == 500;
      logger.info("Loaded {} new app namespaces with startId {}", scanned, maxIdScanned);
    }
  }

  private void mergeAppNamespaces(List<AppNamespace> appNamespaces) {
    for (AppNamespace appNamespace : appNamespaces) {
      // 添加到 `appNamespaceCache` 中
      appNamespaceCache.put(assembleAppNamespaceKey(appNamespace), appNamespace);
      // 添加到 `appNamespaceIdCache`
      appNamespaceIdCache.put(appNamespace.getId(), appNamespace);
      // 若是公用类型，则添加到 `publicAppNamespaceCache` 中
      if (appNamespace.isPublic()) {
        publicAppNamespaceCache.put(appNamespace.getName(), appNamespace);
      }
    }
  }

  //for those updated or deleted app namespaces
  private void updateAndDeleteCache() {
    // 从缓存中，获得所有的 AppNamespace 编号集合
    List<Long> ids = Lists.newArrayList(appNamespaceIdCache.keySet());
    if (CollectionUtils.isEmpty(ids)) {
      return;
    }
    // 每 500 一批，从数据库中查询最新的 AppNamespace 信息
    List<List<Long>> partitionIds = Lists.partition(ids, 500);
    for (List<Long> toRebuild : partitionIds) {
      Iterable<AppNamespace> appNamespaces = appNamespaceRepository.findAllById(toRebuild);

      if (appNamespaces == null) {
        continue;
      }

      // 处理更新的情况
      // handle updated
      Set<Long> foundIds = handleUpdatedAppNamespaces(appNamespaces);

      // 处理删除的情况
      //handle deleted
      handleDeletedAppNamespaces(Sets.difference(Sets.newHashSet(toRebuild), foundIds));
    }
  }

  //for those updated app namespaces
  private Set<Long> handleUpdatedAppNamespaces(Iterable<AppNamespace> appNamespaces) {
    Set<Long> foundIds = Sets.newHashSet();
    for (AppNamespace appNamespace : appNamespaces) {
      foundIds.add(appNamespace.getId());
      // 获得缓存中的 AppNamespace 对象
      AppNamespace thatInCache = appNamespaceIdCache.get(appNamespace.getId());
      // 从 DB 中查询到的 AppNamespace 的更新时间更大，才认为是更新
      if (thatInCache != null && appNamespace.getDataChangeLastModifiedTime().after(thatInCache
          .getDataChangeLastModifiedTime())) {
        // 添加到 appNamespaceIdCache 中
        appNamespaceIdCache.put(appNamespace.getId(), appNamespace);
        String oldKey = assembleAppNamespaceKey(thatInCache);
        String newKey = assembleAppNamespaceKey(appNamespace);
        // 添加到 appNamespaceCache 中
        appNamespaceCache.put(newKey, appNamespace);

        // 当 appId 或 namespaceName 发生改变的情况，将老的移除出 appNamespaceCache
        //in case appId or namespaceName changes
        if (!newKey.equals(oldKey)) {
          appNamespaceCache.remove(oldKey);
        }

        // 添加到 publicAppNamespaceCache 中
        if (appNamespace.isPublic()) {// 新的是公用类型
          // 添加到 publicAppNamespaceCache 中
          publicAppNamespaceCache.put(appNamespace.getName(), appNamespace);

          //in case namespaceName changes
          if (!appNamespace.getName().equals(thatInCache.getName()) && thatInCache.isPublic()) {
            // 当 namespaceName 发生改变的情况，将老的移除出 publicAppNamespaceCache
            publicAppNamespaceCache.remove(thatInCache.getName());
          }
        } else if (thatInCache.isPublic()) {// 新的不是公用类型，需要移除
          //just in case isPublic changes
          publicAppNamespaceCache.remove(thatInCache.getName());
        }
        logger.info("Found AppNamespace changes, old: {}, new: {}", thatInCache, appNamespace);
      }
    }
    return foundIds;
  }

  //for those deleted app namespaces
  private void handleDeletedAppNamespaces(Set<Long> deletedIds) {
    if (CollectionUtils.isEmpty(deletedIds)) {
      return;
    }
    for (Long deletedId : deletedIds) {
      // 从 appNamespaceIdCache 中移除
      AppNamespace deleted = appNamespaceIdCache.remove(deletedId);
      if (deleted == null) {
        continue;
      }
      // 从 appNamespaceCache 中移除
      appNamespaceCache.remove(assembleAppNamespaceKey(deleted));
      // 从 publicAppNamespaceCache 移除
      if (deleted.isPublic()) {
        AppNamespace publicAppNamespace = publicAppNamespaceCache.get(deleted.getName());
        // in case there is some dirty data, e.g. public namespace deleted in some app and now created in another app
        if (publicAppNamespace == deleted) {
          publicAppNamespaceCache.remove(deleted.getName());
        }
      }
      logger.info("Found AppNamespace deleted, {}", deleted);
    }
  }

  private String assembleAppNamespaceKey(AppNamespace appNamespace) {
    return STRING_JOINER.join(appNamespace.getAppId(), appNamespace.getName());
  }

  private void populateDataBaseInterval() {
    scanInterval = bizConfig.appNamespaceCacheScanInterval(); // "apollo.app-namespace-cache-scan.interval"
    scanIntervalTimeUnit = bizConfig.appNamespaceCacheScanIntervalTimeUnit(); // 默认秒，不可配置
    rebuildInterval = bizConfig.appNamespaceCacheRebuildInterval(); // "apollo.app-namespace-cache-rebuild.interval"
    rebuildIntervalTimeUnit = bizConfig.appNamespaceCacheRebuildIntervalTimeUnit(); // 默认秒，不可配置
  }

  //only for test use
  private void reset() throws Exception {
    scheduledExecutorService.shutdownNow();
    initialize();
    afterPropertiesSet();
  }
}
