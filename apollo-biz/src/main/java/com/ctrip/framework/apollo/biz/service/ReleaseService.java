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
package com.ctrip.framework.apollo.biz.service;

import com.ctrip.framework.apollo.biz.entity.Audit;
import com.ctrip.framework.apollo.biz.entity.GrayReleaseRule;
import com.ctrip.framework.apollo.biz.entity.Item;
import com.ctrip.framework.apollo.biz.entity.Namespace;
import com.ctrip.framework.apollo.biz.entity.NamespaceLock;
import com.ctrip.framework.apollo.biz.entity.Release;
import com.ctrip.framework.apollo.biz.entity.ReleaseHistory;
import com.ctrip.framework.apollo.biz.repository.ReleaseRepository;
import com.ctrip.framework.apollo.biz.utils.ReleaseKeyGenerator;
import com.ctrip.framework.apollo.common.constants.GsonType;
import com.ctrip.framework.apollo.common.constants.ReleaseOperation;
import com.ctrip.framework.apollo.common.constants.ReleaseOperationContext;
import com.ctrip.framework.apollo.common.dto.ItemChangeSets;
import com.ctrip.framework.apollo.common.exception.BadRequestException;
import com.ctrip.framework.apollo.common.exception.NotFoundException;
import com.ctrip.framework.apollo.common.utils.GrayReleaseRuleItemTransformer;
import com.ctrip.framework.apollo.core.utils.StringUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang.time.FastDateFormat;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.Type;
import java.util.*;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@Service
public class ReleaseService {

  private static final FastDateFormat TIMESTAMP_FORMAT = FastDateFormat.getInstance("yyyyMMddHHmmss");
  private static final Gson GSON = new Gson();
  private static final Set<Integer> BRANCH_RELEASE_OPERATIONS = Sets
      .newHashSet(ReleaseOperation.GRAY_RELEASE, ReleaseOperation.MASTER_NORMAL_RELEASE_MERGE_TO_GRAY,
          ReleaseOperation.MATER_ROLLBACK_MERGE_TO_GRAY);
  private static final Pageable FIRST_ITEM = PageRequest.of(0, 1);
  private static final Type OPERATION_CONTEXT_TYPE_REFERENCE = new TypeToken<Map<String, Object>>() { }.getType();

  private final ReleaseRepository releaseRepository;
  private final ItemService itemService;
  private final AuditService auditService;
  private final NamespaceLockService namespaceLockService;
  private final NamespaceService namespaceService;
  private final NamespaceBranchService namespaceBranchService;
  private final ReleaseHistoryService releaseHistoryService;
  private final ItemSetService itemSetService;

  public ReleaseService(
      final ReleaseRepository releaseRepository,
      final ItemService itemService,
      final AuditService auditService,
      final NamespaceLockService namespaceLockService,
      final NamespaceService namespaceService,
      final NamespaceBranchService namespaceBranchService,
      final ReleaseHistoryService releaseHistoryService,
      final ItemSetService itemSetService) {
    this.releaseRepository = releaseRepository;
    this.itemService = itemService;
    this.auditService = auditService;
    this.namespaceLockService = namespaceLockService;
    this.namespaceService = namespaceService;
    this.namespaceBranchService = namespaceBranchService;
    this.releaseHistoryService = releaseHistoryService;
    this.itemSetService = itemSetService;
  }

  public Release findOne(long releaseId) {
    return releaseRepository.findById(releaseId).orElse(null);
  }


  public Release findActiveOne(long releaseId) {
    return releaseRepository.findByIdAndIsAbandonedFalse(releaseId);
  }

  public List<Release> findByReleaseIds(Set<Long> releaseIds) {
    Iterable<Release> releases = releaseRepository.findAllById(releaseIds);
    if (releases == null) {
      return Collections.emptyList();
    }
    return Lists.newArrayList(releases);
  }

  public List<Release> findByReleaseKeys(Set<String> releaseKeys) {
    return releaseRepository.findByReleaseKeyIn(releaseKeys);
  }

  // 获得最后、有效的 Release 对象
  public Release findLatestActiveRelease(Namespace namespace) {
    return findLatestActiveRelease(namespace.getAppId(),
                                   namespace.getClusterName(), namespace.getNamespaceName());

  }

  public Release findLatestActiveRelease(String appId, String clusterName, String namespaceName) {
    return releaseRepository.findFirstByAppIdAndClusterNameAndNamespaceNameAndIsAbandonedFalseOrderByIdDesc(appId,
                                                                                                            clusterName,
                                                                                                            namespaceName);
  }

  public List<Release> findAllReleases(String appId, String clusterName, String namespaceName, Pageable page) {
    List<Release> releases = releaseRepository.findByAppIdAndClusterNameAndNamespaceNameOrderByIdDesc(appId,
                                                                                                      clusterName,
                                                                                                      namespaceName,
                                                                                                      page);
    if (releases == null) {
      return Collections.emptyList();
    }
    return releases;
  }

  public List<Release> findActiveReleases(String appId, String clusterName, String namespaceName, Pageable page) {
    List<Release>
        releases =
        releaseRepository.findByAppIdAndClusterNameAndNamespaceNameAndIsAbandonedFalseOrderByIdDesc(appId, clusterName,
                                                                                                    namespaceName,
                                                                                                    page);
    if (releases == null) {
      return Collections.emptyList();
    }
    return releases;
  }

  private List<Release> findActiveReleasesBetween(String appId, String clusterName, String namespaceName,
                                                  long fromReleaseId, long toReleaseId) {
    List<Release>
        releases =
        releaseRepository.findByAppIdAndClusterNameAndNamespaceNameAndIsAbandonedFalseAndIdBetweenOrderByIdDesc(appId,
                                                                                                                clusterName,
                                                                                                                namespaceName,
                                                                                                                fromReleaseId,
                                                                                                                toReleaseId);
    if (releases == null) {
      return Collections.emptyList();
    }
    return releases;
  }

  // 合并子 Namespace 变更的配置 Map 到父 Namespace ，并进行一次 Release
  @Transactional
  public Release mergeBranchChangeSetsAndRelease(Namespace namespace, String branchName, String releaseName,
                                                 String releaseComment, boolean isEmergencyPublish,
                                                 ItemChangeSets changeSets) {

    // 校验锁定
    checkLock(namespace, isEmergencyPublish, changeSets.getDataChangeLastModifiedBy());

    // 变更的配置集 合 ItemChangeSets 对象，更新到父 Namespace 中。
    itemSetService.updateSet(namespace, changeSets);

    // 获得子 Namespace 的最新且有效的 Release 对象
    Release branchRelease = findLatestActiveRelease(namespace.getAppId(), branchName, namespace
        .getNamespaceName());
    // 获得子 Namespace 的最新且有效的 Release 编号
    long branchReleaseId = branchRelease == null ? 0 : branchRelease.getId();

    // 获得父 Namespace 的配置 Map
    // 因为上面已经更新过，所以获得到的是合并后的结果
    Map<String, String> operateNamespaceItems = getNamespaceItems(namespace);

    // 创建 Map ，用于 ReleaseHistory 对象的 `operationContext` 属性。
    Map<String, Object> operationContext = Maps.newLinkedHashMap();
    operationContext.put(ReleaseOperationContext.SOURCE_BRANCH, branchName);
    operationContext.put(ReleaseOperationContext.BASE_RELEASE_ID, branchReleaseId);
    operationContext.put(ReleaseOperationContext.IS_EMERGENCY_PUBLISH, isEmergencyPublish);

    // 父 Namespace 进行发布
    return masterRelease(namespace, releaseName, releaseComment, operateNamespaceItems,
                         changeSets.getDataChangeLastModifiedBy(),
                         ReleaseOperation.GRAY_RELEASE_MERGE_TO_MASTER, operationContext);

  }

  @Transactional
  public Release publish(Namespace namespace, String releaseName, String releaseComment,
                         String operator, boolean isEmergencyPublish) {
    // 校验锁定
    checkLock(namespace, isEmergencyPublish, operator);
    // 获得 Namespace 的普通配置 Map
    Map<String, String> operateNamespaceItems = getNamespaceItems(namespace);
    // 获得父 Namespace
    Namespace parentNamespace = namespaceService.findParentNamespace(namespace);

    //branch release
    // 当前namespace的父namespace不为空，合并父namespace发布子namespace
    // 灰度发布
    if (parentNamespace != null) {
      return publishBranchNamespace(parentNamespace, namespace, operateNamespaceItems,
                                    releaseName, releaseComment, operator, isEmergencyPublish);
    }

    // 当前namespace的父namespace为空，查找当前namespace的子namespace
    // 获得子 Namespace 对象
    Namespace childNamespace = namespaceService.findChildNamespace(namespace);

    // 获得上一次，并且有效的 Release 对象
    Release previousRelease = null;
    if (childNamespace != null) {
      previousRelease = findLatestActiveRelease(namespace);
    }

    // 创建操作 Context
    //master release
    Map<String, Object> operationContext = Maps.newLinkedHashMap();
    operationContext.put(ReleaseOperationContext.IS_EMERGENCY_PUBLISH, isEmergencyPublish);

    // 发布当前namespace
    Release release = masterRelease(namespace, releaseName, releaseComment, operateNamespaceItems,
                                    operator, ReleaseOperation.NORMAL_RELEASE, operationContext);

    //merge to branch and auto release
    // 当前namespace发布完成后发布子namespace
    // 灰度发布
    if (childNamespace != null) {
      mergeFromMasterAndPublishBranch(namespace, childNamespace, operateNamespaceItems,
                                      releaseName, releaseComment, operator, previousRelease,
                                      release, isEmergencyPublish);
    }

    return release;
  }

  private Release publishBranchNamespace(Namespace parentNamespace, Namespace childNamespace,
                                         Map<String, String> childNamespaceItems,
                                         String releaseName, String releaseComment,
                                         String operator, boolean isEmergencyPublish, Set<String> grayDelKeys) {
    // 获得父 Namespace 的最后有效 Release 对象
    Release parentLatestRelease = findLatestActiveRelease(parentNamespace);
    // 获得父 Namespace 的配置项
    Map<String, String> parentConfigurations = parentLatestRelease != null ?
            GSON.fromJson(parentLatestRelease.getConfigurations(),
                          GsonType.CONFIG) : new LinkedHashMap<>();
    // 获得父 Namespace 的 releaseId 属性
    long baseReleaseId = parentLatestRelease == null ? 0 : parentLatestRelease.getId();

    // 合并配置项
    Map<String, String> configsToPublish = mergeConfiguration(parentConfigurations, childNamespaceItems);

    if(!(grayDelKeys == null || grayDelKeys.size()==0)){
      for (String key : grayDelKeys){
        configsToPublish.remove(key);
      }
    }

    // 发布子 Namespace 的 Release
    return branchRelease(parentNamespace, childNamespace, releaseName, releaseComment,
        configsToPublish, baseReleaseId, operator, ReleaseOperation.GRAY_RELEASE, isEmergencyPublish,
        childNamespaceItems.keySet());

  }

  @Transactional
  public Release grayDeletionPublish(Namespace namespace, String releaseName, String releaseComment,
                                     String operator, boolean isEmergencyPublish, Set<String> grayDelKeys) {

    checkLock(namespace, isEmergencyPublish, operator);

    Map<String, String> operateNamespaceItems = getNamespaceItems(namespace);

    Namespace parentNamespace = namespaceService.findParentNamespace(namespace);

    //branch release
    if (parentNamespace != null) {
      return publishBranchNamespace(parentNamespace, namespace, operateNamespaceItems,
              releaseName, releaseComment, operator, isEmergencyPublish, grayDelKeys);
    }
    throw new NotFoundException("Parent namespace not found");
  }

  // 校验 NamespaceLock 锁定
  private void checkLock(Namespace namespace, boolean isEmergencyPublish, String operator) {
    if (!isEmergencyPublish) {
      // 获得 NamespaceLock 对象
      NamespaceLock lock = namespaceLockService.findLock(namespace.getId());
      // 校验锁定人是否是当前管理员。若是，抛出 BadRequestException 异常
      if (lock != null && lock.getDataChangeCreatedBy().equals(operator)) {
        throw new BadRequestException("Config can not be published by yourself.");
      }
    }
  }

  // 在父 Namespace 发布 Release 后，会调用 #mergeFromMasterAndPublishBranch(...) 方法，自动将 父 Namespace (主干) 合并到子 Namespace (分支)，并进行一次子 Namespace 的发布
  private void mergeFromMasterAndPublishBranch(Namespace parentNamespace, Namespace childNamespace,
                                               Map<String, String> parentNamespaceItems,
                                               String releaseName, String releaseComment,
                                               String operator, Release masterPreviousRelease,
                                               Release parentRelease, boolean isEmergencyPublish) {
    //create release for child namespace
    Release childNamespaceLatestActiveRelease = findLatestActiveRelease(childNamespace);

    // 获得子 Namespace 的配置 Map
    Map<String, String> childReleaseConfiguration;
    Collection<String> branchReleaseKeys;
    if (childNamespaceLatestActiveRelease != null) {
      childReleaseConfiguration = GSON.fromJson(childNamespaceLatestActiveRelease.getConfigurations(), GsonType.CONFIG);
      branchReleaseKeys = getBranchReleaseKeys(childNamespaceLatestActiveRelease.getId());
    } else {
      childReleaseConfiguration = Collections.emptyMap();
      branchReleaseKeys = null;
    }

    // 获得父 Namespace 的配置 Map
    Map<String, String> parentNamespaceOldConfiguration = masterPreviousRelease == null ?
                                                          null : GSON.fromJson(masterPreviousRelease.getConfigurations(),
                                                                               GsonType.CONFIG);

    // 计算合并最新父 Namespace 的配置 Map 后的子 Namespace 的配置 Map
    Map<String, String> childNamespaceToPublishConfigs =
        calculateChildNamespaceToPublishConfiguration(parentNamespaceOldConfiguration, parentNamespaceItems,
            childReleaseConfiguration, branchReleaseKeys);

    //compare
    // 若发生了变化，则进行一次子 Namespace 的发布
    if (!childNamespaceToPublishConfigs.equals(childReleaseConfiguration)) {
      branchRelease(parentNamespace, childNamespace, releaseName, releaseComment,
                    childNamespaceToPublishConfigs, parentRelease.getId(), operator,
                    ReleaseOperation.MASTER_NORMAL_RELEASE_MERGE_TO_GRAY, isEmergencyPublish, branchReleaseKeys);
    }

  }

  private Collection<String> getBranchReleaseKeys(long releaseId) {
    Page<ReleaseHistory> releaseHistories = releaseHistoryService
        .findByReleaseIdAndOperationInOrderByIdDesc(releaseId, BRANCH_RELEASE_OPERATIONS, FIRST_ITEM);

    if (!releaseHistories.hasContent()) {
      return null;
    }

    Map<String, Object> operationContext = GSON
        .fromJson(releaseHistories.getContent().get(0).getOperationContext(), OPERATION_CONTEXT_TYPE_REFERENCE);

    if (operationContext == null || !operationContext.containsKey(ReleaseOperationContext.BRANCH_RELEASE_KEYS)) {
      return null;
    }

    return (Collection<String>) operationContext.get(ReleaseOperationContext.BRANCH_RELEASE_KEYS);
  }

  private Release publishBranchNamespace(Namespace parentNamespace, Namespace childNamespace,
                                         Map<String, String> childNamespaceItems,
                                         String releaseName, String releaseComment,
                                         String operator, boolean isEmergencyPublish) {
    return publishBranchNamespace(parentNamespace, childNamespace, childNamespaceItems, releaseName, releaseComment,
            operator, isEmergencyPublish, null);

  }

  private Release masterRelease(Namespace namespace, String releaseName, String releaseComment,
                                Map<String, String> configurations, String operator,
                                int releaseOperation, Map<String, Object> operationContext) {
    // 获得最后有效的 Release 对象
    Release lastActiveRelease = findLatestActiveRelease(namespace);
    long previousReleaseId = lastActiveRelease == null ? 0 : lastActiveRelease.getId();
    // 创建 Release 对象，并保存
    Release release = createRelease(namespace, releaseName, releaseComment,
                                    configurations, operator);

    // 创建 ReleaseHistory 对象，并保存
    releaseHistoryService.createReleaseHistory(namespace.getAppId(), namespace.getClusterName(),
                                               namespace.getNamespaceName(), namespace.getClusterName(),
                                               release.getId(), previousReleaseId, releaseOperation,
                                               operationContext, operator);

    return release;
  }

  /**
   * 发布Namespace 的 Release
   *
   * @param parentNamespace
   * @param childNamespace
   * @param releaseName
   * @param releaseComment
   * @param configurations
   * @param baseReleaseId
   * @param operator
   * @param releaseOperation
   * @param isEmergencyPublish
   * @param branchReleaseKeys
   * @return
   */
  private Release branchRelease(Namespace parentNamespace, Namespace childNamespace,
                                String releaseName, String releaseComment,
                                Map<String, String> configurations, long baseReleaseId,
                                String operator, int releaseOperation, boolean isEmergencyPublish, Collection<String> branchReleaseKeys) {
    // 获得父 Namespace 最后有效的 Release 对象
    Release previousRelease = findLatestActiveRelease(childNamespace.getAppId(),
                                                      childNamespace.getClusterName(),
                                                      childNamespace.getNamespaceName());
    // 获得父 Namespace 最后有效的 Release 对象的编号
    long previousReleaseId = previousRelease == null ? 0 : previousRelease.getId();

    // 创建 Map ，用于 ReleaseHistory 对象的 `operationContext` 属性。
    Map<String, Object> releaseOperationContext = Maps.newLinkedHashMap();
    releaseOperationContext.put(ReleaseOperationContext.BASE_RELEASE_ID, baseReleaseId);
    releaseOperationContext.put(ReleaseOperationContext.IS_EMERGENCY_PUBLISH, isEmergencyPublish);
    releaseOperationContext.put(ReleaseOperationContext.BRANCH_RELEASE_KEYS, branchReleaseKeys);

    // 创建子 Namespace 的 Release 对象，并保存
    Release release =
        createRelease(childNamespace, releaseName, releaseComment, configurations, operator);

    // 更新 GrayReleaseRule 的 releaseId 属性
    //update gray release rules
    GrayReleaseRule grayReleaseRule = namespaceBranchService.updateRulesReleaseId(childNamespace.getAppId(),
                                                                                  parentNamespace.getClusterName(),
                                                                                  childNamespace.getNamespaceName(),
                                                                                  childNamespace.getClusterName(),
                                                                                  release.getId(), operator);

    if (grayReleaseRule != null) {
      releaseOperationContext.put(ReleaseOperationContext.RULES, GrayReleaseRuleItemTransformer
          .batchTransformFromJSON(grayReleaseRule.getRules()));
    }

    // 创建 ReleaseHistory 对象，并保存到数据库中
    releaseHistoryService.createReleaseHistory(parentNamespace.getAppId(), parentNamespace.getClusterName(),
                                               parentNamespace.getNamespaceName(), childNamespace.getClusterName(),
                                               release.getId(),
                                               previousReleaseId, releaseOperation, releaseOperationContext, operator);

    return release;
  }

  private Map<String, String> mergeConfiguration(Map<String, String> baseConfigurations,
                                                 Map<String, String> coverConfigurations) {
    Map<String, String> result = new LinkedHashMap<>();
    //copy base configuration
    // 父 Namespace 的配置项
    for (Map.Entry<String, String> entry : baseConfigurations.entrySet()) {
      result.put(entry.getKey(), entry.getValue());
    }

    //update and publish
    // 子 Namespace 的配置项(覆盖父配置项的)
    for (Map.Entry<String, String> entry : coverConfigurations.entrySet()) {
      result.put(entry.getKey(), entry.getValue());
    }

    // 返回合并后的配置项
    return result;
  }


  private Map<String, String> getNamespaceItems(Namespace namespace) {
    // 读取 Namespace 的 Item 集合
    List<Item> items = itemService.findItemsWithOrdered(namespace.getId());
    // 生成普通配置 Map 。过滤掉注释和空行的配置项
    Map<String, String> configurations = new LinkedHashMap<>();
    for (Item item : items) {
      if (StringUtils.isEmpty(item.getKey())) {
        continue;
      }
      configurations.put(item.getKey(), item.getValue());
    }

    return configurations;
  }

  // 创建 Release 对象，并保存
  private Release createRelease(Namespace namespace, String name, String comment,
                                Map<String, String> configurations, String operator) {
    Release release = new Release();
    release.setReleaseKey(ReleaseKeyGenerator.generateReleaseKey(namespace));
    release.setDataChangeCreatedTime(new Date());
    release.setDataChangeCreatedBy(operator);
    release.setDataChangeLastModifiedBy(operator);
    release.setName(name);
    release.setComment(comment);
    release.setAppId(namespace.getAppId());
    release.setClusterName(namespace.getClusterName());
    release.setNamespaceName(namespace.getNamespaceName());
    release.setConfigurations(GSON.toJson(configurations));
    release = releaseRepository.save(release);

    // 释放 NamespaceLock
    namespaceLockService.unlock(namespace.getId());
    auditService.audit(Release.class.getSimpleName(), release.getId(), Audit.OP.INSERT,
                       release.getDataChangeCreatedBy());

    return release;
  }

  @Transactional
  public Release rollback(long releaseId, String operator) {
    Release release = findOne(releaseId);
    if (release == null) {
      throw new NotFoundException("release not found");
    }
    if (release.isAbandoned()) {
      throw new BadRequestException("release is not active");
    }

    String appId = release.getAppId();
    String clusterName = release.getClusterName();
    String namespaceName = release.getNamespaceName();

    PageRequest page = PageRequest.of(0, 2);
    List<Release> twoLatestActiveReleases = findActiveReleases(appId, clusterName, namespaceName, page);
    if (twoLatestActiveReleases == null || twoLatestActiveReleases.size() < 2) {
      throw new BadRequestException(String.format(
          "Can't rollback namespace(appId=%s, clusterName=%s, namespaceName=%s) because there is only one active release",
          appId,
          clusterName,
          namespaceName));
    }

    release.setAbandoned(true);
    release.setDataChangeLastModifiedBy(operator);

    releaseRepository.save(release);

    releaseHistoryService.createReleaseHistory(appId, clusterName,
                                               namespaceName, clusterName, twoLatestActiveReleases.get(1).getId(),
                                               release.getId(), ReleaseOperation.ROLLBACK, null, operator);

    //publish child namespace if namespace has child
    rollbackChildNamespace(appId, clusterName, namespaceName, twoLatestActiveReleases, operator);

    return release;
  }

  @Transactional
  public Release rollbackTo(long releaseId, long toReleaseId, String operator) {
    if (releaseId == toReleaseId) {
      throw new BadRequestException("current release equal to target release");
    }

    Release release = findOne(releaseId);
    Release toRelease = findOne(toReleaseId);
    if (release == null || toRelease == null) {
      throw new NotFoundException("release not found");
    }
    if (release.isAbandoned() || toRelease.isAbandoned()) {
      throw new BadRequestException("release is not active");
    }

    String appId = release.getAppId();
    String clusterName = release.getClusterName();
    String namespaceName = release.getNamespaceName();

    List<Release> releases = findActiveReleasesBetween(appId, clusterName, namespaceName,
                                                       toReleaseId, releaseId);

    for (int i = 0; i < releases.size() - 1; i++) {
      releases.get(i).setAbandoned(true);
      releases.get(i).setDataChangeLastModifiedBy(operator);
    }

    releaseRepository.saveAll(releases);

    releaseHistoryService.createReleaseHistory(appId, clusterName,
                                               namespaceName, clusterName, toReleaseId,
                                               release.getId(), ReleaseOperation.ROLLBACK, null, operator);

    //publish child namespace if namespace has child
    rollbackChildNamespace(appId, clusterName, namespaceName, Lists.newArrayList(release, toRelease), operator);

    return release;
  }

  private void rollbackChildNamespace(String appId, String clusterName, String namespaceName,
                                      List<Release> parentNamespaceTwoLatestActiveRelease, String operator) {
    Namespace parentNamespace = namespaceService.findOne(appId, clusterName, namespaceName);
    Namespace childNamespace = namespaceService.findChildNamespace(appId, clusterName, namespaceName);
    if (parentNamespace == null || childNamespace == null) {
      return;
    }

    Release childNamespaceLatestActiveRelease = findLatestActiveRelease(childNamespace);
    Map<String, String> childReleaseConfiguration;
    Collection<String> branchReleaseKeys;
    if (childNamespaceLatestActiveRelease != null) {
      childReleaseConfiguration = GSON.fromJson(childNamespaceLatestActiveRelease.getConfigurations(), GsonType.CONFIG);
      branchReleaseKeys = getBranchReleaseKeys(childNamespaceLatestActiveRelease.getId());
    } else {
      childReleaseConfiguration = Collections.emptyMap();
      branchReleaseKeys = null;
    }

    Release abandonedRelease = parentNamespaceTwoLatestActiveRelease.get(0);
    Release parentNamespaceNewLatestRelease = parentNamespaceTwoLatestActiveRelease.get(1);

    Map<String, String> parentNamespaceAbandonedConfiguration = GSON.fromJson(abandonedRelease.getConfigurations(),
                                                                              GsonType.CONFIG);

    Map<String, String>
        parentNamespaceNewLatestConfiguration =
        GSON.fromJson(parentNamespaceNewLatestRelease.getConfigurations(), GsonType.CONFIG);

    Map<String, String>
        childNamespaceNewConfiguration =
        calculateChildNamespaceToPublishConfiguration(parentNamespaceAbandonedConfiguration,
            parentNamespaceNewLatestConfiguration, childReleaseConfiguration, branchReleaseKeys);

    //compare
    if (!childNamespaceNewConfiguration.equals(childReleaseConfiguration)) {
      branchRelease(parentNamespace, childNamespace,
          TIMESTAMP_FORMAT.format(new Date()) + "-master-rollback-merge-to-gray", "",
          childNamespaceNewConfiguration, parentNamespaceNewLatestRelease.getId(), operator,
          ReleaseOperation.MATER_ROLLBACK_MERGE_TO_GRAY, false, branchReleaseKeys);
    }
  }

  // 计算合并最新父 Namespace 的配置 Map 后的子 Namespace 的配置 Map
  private Map<String, String> calculateChildNamespaceToPublishConfiguration(
      Map<String, String> parentNamespaceOldConfiguration, Map<String, String> parentNamespaceNewConfiguration,
      Map<String, String> childNamespaceLatestActiveConfiguration, Collection<String> branchReleaseKeys) {
    //first. calculate child namespace modified configs

    // 以子 Namespace 的配置 Map 为基础，计算出差异的 Map
    Map<String, String> childNamespaceModifiedConfiguration = calculateBranchModifiedItemsAccordingToRelease(
        parentNamespaceOldConfiguration, childNamespaceLatestActiveConfiguration, branchReleaseKeys);

    //second. append child namespace modified configs to parent namespace new latest configuration
    return mergeConfiguration(parentNamespaceNewConfiguration, childNamespaceModifiedConfiguration);
  }

  // 以子 Namespace 的配置 Map 为基础，计算出差异的 Map
  // 子 Namespace 的配置 Map 是包含老的父 Namespace 的配置 Map ，所以需要剔除。但是呢，剔除的过程中，又需要保留子 Namespace 的自定义的配置项，所以以子Namespace为主进行put
  private Map<String, String> calculateBranchModifiedItemsAccordingToRelease(
      Map<String, String> masterReleaseConfigs, Map<String, String> branchReleaseConfigs,
      Collection<String> branchReleaseKeys) {

    // 差异 Map
    Map<String, String> modifiedConfigs = new LinkedHashMap<>();

    // 若子 Namespace 的配置 Map 为空，直接返回空 Map
    if (CollectionUtils.isEmpty(branchReleaseConfigs)) {
      return modifiedConfigs;
    }

    // 若父 Namespace 的配置 Map 为空，直接返回子 Namespace 的配置 Map
    // 以子 Namespace 的配置 Map 为基础，计算出差异的 Map
    // new logic, retrieve modified configurations based on branch release keys
    if (branchReleaseKeys != null) {
      for (String branchReleaseKey : branchReleaseKeys) {
        if (branchReleaseConfigs.containsKey(branchReleaseKey)) {
          modifiedConfigs.put(branchReleaseKey, branchReleaseConfigs.get(branchReleaseKey));
        }
      }

      return modifiedConfigs;
    }

    // old logic, retrieve modified configurations by comparing branchReleaseConfigs with masterReleaseConfigs
    if (CollectionUtils.isEmpty(masterReleaseConfigs)) {
      return branchReleaseConfigs;
    }

    for (Map.Entry<String, String> entry : branchReleaseConfigs.entrySet()) {

      if (!Objects.equals(entry.getValue(), masterReleaseConfigs.get(entry.getKey()))) {
        modifiedConfigs.put(entry.getKey(), entry.getValue());
      }
    }

    return modifiedConfigs;

  }

  @Transactional
  public int batchDelete(String appId, String clusterName, String namespaceName, String operator) {
    return releaseRepository.batchDelete(appId, clusterName, namespaceName, operator);
  }

}
