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
package com.ctrip.framework.apollo.portal.service;

import com.ctrip.framework.apollo.common.constants.GsonType;
import com.ctrip.framework.apollo.common.dto.ItemDTO;
import com.ctrip.framework.apollo.common.dto.NamespaceDTO;
import com.ctrip.framework.apollo.common.dto.ReleaseDTO;
import com.ctrip.framework.apollo.common.entity.AppNamespace;
import com.ctrip.framework.apollo.common.exception.BadRequestException;
import com.ctrip.framework.apollo.common.utils.BeanUtils;
import com.ctrip.framework.apollo.core.enums.ConfigFileFormat;
import com.ctrip.framework.apollo.core.utils.StringUtils;
import com.ctrip.framework.apollo.portal.api.AdminServiceAPI;
import com.ctrip.framework.apollo.portal.component.PortalSettings;
import com.ctrip.framework.apollo.portal.component.config.PortalConfig;
import com.ctrip.framework.apollo.portal.constant.RoleType;
import com.ctrip.framework.apollo.portal.constant.TracerEventType;
import com.ctrip.framework.apollo.portal.enricher.adapter.BaseDtoUserInfoEnrichedAdapter;
import com.ctrip.framework.apollo.portal.entity.bo.ItemBO;
import com.ctrip.framework.apollo.portal.entity.bo.NamespaceBO;
import com.ctrip.framework.apollo.portal.environment.Env;
import com.ctrip.framework.apollo.portal.spi.UserInfoHolder;
import com.ctrip.framework.apollo.portal.util.RoleUtils;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class NamespaceService {

  private Logger logger = LoggerFactory.getLogger(NamespaceService.class);
  private static final Gson GSON = new Gson();

  private final PortalConfig portalConfig;
  private final PortalSettings portalSettings;
  private final UserInfoHolder userInfoHolder;
  private final AdminServiceAPI.NamespaceAPI namespaceAPI;
  private final ItemService itemService;
  private final ReleaseService releaseService;
  private final AppNamespaceService appNamespaceService;
  private final InstanceService instanceService;
  private final NamespaceBranchService branchService;
  private final RolePermissionService rolePermissionService;
  private final AdditionalUserInfoEnrichService additionalUserInfoEnrichService;

  public NamespaceService(
      final PortalConfig portalConfig,
      final PortalSettings portalSettings,
      final UserInfoHolder userInfoHolder,
      final AdminServiceAPI.NamespaceAPI namespaceAPI,
      final ItemService itemService,
      final ReleaseService releaseService,
      final AppNamespaceService appNamespaceService,
      final InstanceService instanceService,
      final @Lazy NamespaceBranchService branchService,
      final RolePermissionService rolePermissionService,
      final AdditionalUserInfoEnrichService additionalUserInfoEnrichService) {
    this.portalConfig = portalConfig;
    this.portalSettings = portalSettings;
    this.userInfoHolder = userInfoHolder;
    this.namespaceAPI = namespaceAPI;
    this.itemService = itemService;
    this.releaseService = releaseService;
    this.appNamespaceService = appNamespaceService;
    this.instanceService = instanceService;
    this.branchService = branchService;
    this.rolePermissionService = rolePermissionService;
    this.additionalUserInfoEnrichService = additionalUserInfoEnrichService;
  }


  public NamespaceDTO createNamespace(Env env, NamespaceDTO namespace) {
    if (StringUtils.isEmpty(namespace.getDataChangeCreatedBy())) {
      namespace.setDataChangeCreatedBy(userInfoHolder.getUser().getUserId());
    }
    namespace.setDataChangeLastModifiedBy(userInfoHolder.getUser().getUserId());
    NamespaceDTO createdNamespace = namespaceAPI.createNamespace(env, namespace);

    Tracer.logEvent(TracerEventType.CREATE_NAMESPACE,
        String.format("%s+%s+%s+%s", namespace.getAppId(), env, namespace.getClusterName(),
            namespace.getNamespaceName()));
    return createdNamespace;
  }


  @Transactional
  public void deleteNamespace(String appId, Env env, String clusterName, String namespaceName) {

    AppNamespace appNamespace = appNamespaceService.findByAppIdAndName(appId, namespaceName);

    //1. check parent namespace has not instances
    if (namespaceHasInstances(appId, env, clusterName, namespaceName)) {
      throw new BadRequestException(
          "Can not delete namespace because namespace has active instances");
    }

    //2. check child namespace has not instances
    NamespaceDTO childNamespace = branchService
        .findBranchBaseInfo(appId, env, clusterName, namespaceName);
    if (childNamespace != null &&
        namespaceHasInstances(appId, env, childNamespace.getClusterName(), namespaceName)) {
      throw new BadRequestException(
          "Can not delete namespace because namespace's branch has active instances");
    }

    //3. check public namespace has not associated namespace
    if (appNamespace != null && appNamespace.isPublic() && publicAppNamespaceHasAssociatedNamespace(
        namespaceName, env)) {
      throw new BadRequestException(
          "Can not delete public namespace which has associated namespaces");
    }

    String operator = userInfoHolder.getUser().getUserId();

    namespaceAPI.deleteNamespace(env, appId, clusterName, namespaceName, operator);
  }

  public NamespaceDTO loadNamespaceBaseInfo(String appId, Env env, String clusterName,
      String namespaceName) {
    NamespaceDTO namespace = namespaceAPI.loadNamespace(appId, env, clusterName, namespaceName);
    if (namespace == null) {
      throw new BadRequestException(String.format("Namespace: %s not exist.", namespaceName));
    }
    return namespace;
  }

  /**
   * load cluster all namespace info with items
   */
  public List<NamespaceBO> findNamespaceBOs(String appId, Env env, String clusterName) {

    List<NamespaceDTO> namespaces = namespaceAPI.findNamespaceByCluster(appId, env, clusterName);
    if (namespaces == null || namespaces.size() == 0) {
      throw new BadRequestException("namespaces not exist");
    }

    List<NamespaceBO> namespaceBOs = new LinkedList<>();
    for (NamespaceDTO namespace : namespaces) {

      NamespaceBO namespaceBO;
      try {
        namespaceBO = transformNamespace2BO(env, namespace);
        namespaceBOs.add(namespaceBO);
      } catch (Exception e) {
        logger.error("parse namespace error. app id:{}, env:{}, clusterName:{}, namespace:{}",
            appId, env, clusterName, namespace.getNamespaceName(), e);
        throw e;
      }
    }

    return namespaceBOs;
  }

  public List<NamespaceDTO> findNamespaces(String appId, Env env, String clusterName) {
    return namespaceAPI.findNamespaceByCluster(appId, env, clusterName);
  }

  public List<NamespaceDTO> getPublicAppNamespaceAllNamespaces(Env env, String publicNamespaceName,
      int page,
      int size) {
    return namespaceAPI.getPublicAppNamespaceAllNamespaces(env, publicNamespaceName, page, size);
  }

  public NamespaceBO loadNamespaceBO(String appId, Env env, String clusterName,
      String namespaceName) {
    NamespaceDTO namespace = namespaceAPI.loadNamespace(appId, env, clusterName, namespaceName);
    if (namespace == null) {
      throw new BadRequestException("namespaces not exist");
    }
    return transformNamespace2BO(env, namespace);
  }

  public boolean namespaceHasInstances(String appId, Env env, String clusterName,
      String namespaceName) {
    return instanceService.getInstanceCountByNamepsace(appId, env, clusterName, namespaceName) > 0;
  }

  public boolean publicAppNamespaceHasAssociatedNamespace(String publicNamespaceName, Env env) {
    return namespaceAPI.countPublicAppNamespaceAssociatedNamespaces(env, publicNamespaceName) > 0;
  }

  public NamespaceBO findPublicNamespaceForAssociatedNamespace(Env env, String appId,
      String clusterName, String namespaceName) {
    NamespaceDTO namespace =
        namespaceAPI
            .findPublicNamespaceForAssociatedNamespace(env, appId, clusterName, namespaceName);

    return transformNamespace2BO(env, namespace);
  }

  public Map<String, Map<String, Boolean>> getNamespacesPublishInfo(String appId) {
    Map<String, Map<String, Boolean>> result = Maps.newHashMap();

    Set<Env> envs = portalConfig.publishTipsSupportedEnvs();
    for (Env env : envs) {
      if (portalSettings.isEnvActive(env)) {
        result.put(env.toString(), namespaceAPI.getNamespacePublishInfo(env, appId));
      }
    }

    return result;
  }

  private NamespaceBO transformNamespace2BO(Env env, NamespaceDTO namespace) {
    // 创建 NamespaceBO 对象
    NamespaceBO namespaceBO = new NamespaceBO();
    // 设置 `baseInfo` 属性
    namespaceBO.setBaseInfo(namespace);

    String appId = namespace.getAppId();
    String clusterName = namespace.getClusterName();
    String namespaceName = namespace.getNamespaceName();

    // 填充 NamespaceBO 对象的基础属性
    fillAppNamespaceProperties(namespaceBO);

    List<ItemBO> itemBOs = new LinkedList<>();
    namespaceBO.setItems(itemBOs);

    // 获得最新的已经发布的 Release 的配置 Map
    //latest Release
    ReleaseDTO latestRelease;
    Map<String, String> releaseItems = new HashMap<>();
    Map<String, ItemDTO> deletedItemDTOs = new HashMap<>();
    latestRelease = releaseService.loadLatestRelease(appId, env, clusterName, namespaceName);
    if (latestRelease != null) {
      releaseItems = GSON.fromJson(latestRelease.getConfigurations(), GsonType.CONFIG);
    }

    // 获得 Namespace 所有的 Item 数组
    //not Release config items
    List<ItemDTO> items = itemService.findItems(appId, env, clusterName, namespaceName);
    additionalUserInfoEnrichService
        .enrichAdditionalUserInfo(items, BaseDtoUserInfoEnrichedAdapter::new);
    int modifiedItemCnt = 0;
    // 添加新增或更新的 ItemBO 到 `itemBOs` 中
    for (ItemDTO itemDTO : items) {
      // 创建 ItemBO 对象
      ItemBO itemBO = transformItem2BO(itemDTO, releaseItems);
      // 若有修改，计数增加
      if (itemBO.isModified()) {
        modifiedItemCnt++;
      }
      // 添加到 `itemBOs`
      itemBOs.add(itemBO);
    }

    // 添加已删除的 ItemBO 到 `itemBOs` 中
    //deleted items
    // 计算已经删除的 ItemBO 数组
    itemService.findDeletedItems(appId, env, clusterName, namespaceName).forEach(item -> {
      deletedItemDTOs.put(item.getKey(),item);
    });

    List<ItemBO> deletedItems = parseDeletedItems(items, releaseItems, deletedItemDTOs);
    // 添加到 `itemBOs`
    itemBOs.addAll(deletedItems);
    // 若有删除，计数增加
    modifiedItemCnt += deletedItems.size();

    // 设置 `itemModifiedCnt` 属性
    namespaceBO.setItemModifiedCnt(modifiedItemCnt);

    return namespaceBO;
  }

  // 填充 NamespaceBO 对象的基础属性
  private void fillAppNamespaceProperties(NamespaceBO namespace) {

    final NamespaceDTO namespaceDTO = namespace.getBaseInfo();
    final String appId = namespaceDTO.getAppId();
    final String clusterName = namespaceDTO.getClusterName();
    final String namespaceName = namespaceDTO.getNamespaceName();
    //先从当前appId下面找,包含私有的和公共的
    AppNamespace appNamespace =
        appNamespaceService
            .findByAppIdAndName(appId, namespaceName);
    //再从公共的app namespace里面找
    if (appNamespace == null) {
      appNamespace = appNamespaceService.findPublicAppNamespace(namespaceName);
    }

    // 设置 `format` `public` `parentAppId` `comment` 属性到 NamespaceBO 对象
    final String format;
    final boolean isPublic;
    if (appNamespace == null) {
      //dirty data
      logger.warn("Dirty data, cannot find appNamespace by namespaceName [{}], appId = {}, cluster = {}, set it format to {}, make public", namespaceName, appId, clusterName, ConfigFileFormat.Properties.getValue());
      format = ConfigFileFormat.Properties.getValue();
      isPublic = true; // set to true, because public namespace allowed to delete by user
    } else {
      format = appNamespace.getFormat();
      isPublic = appNamespace.isPublic();
      namespace.setParentAppId(appNamespace.getAppId());
      namespace.setComment(appNamespace.getComment());
    }
    namespace.setFormat(format);
    namespace.setPublic(isPublic);
  }

  // 对比两个集合，计算从 releaseItems 中删除了的配置结果数组
  private List<ItemBO> parseDeletedItems(List<ItemDTO> newItems, Map<String, String> releaseItems, Map<String, ItemDTO> deletedItemDTOs) {
    // 创建新的配置项 Map
    Map<String, ItemDTO> newItemMap = BeanUtils.mapByKey("key", newItems);
    // 创建，已删除的 ItemBO 数组
    List<ItemBO> deletedItems = new LinkedList<>();
    // 循环已发布的配置 Map ，对比哪些配置被删除了
    for (Map.Entry<String, String> entry : releaseItems.entrySet()) {
      String key = entry.getKey();
      if (newItemMap.get(key) == null) {
        // 创建 ItemBO 对象
        ItemBO deletedItem = new ItemBO();

        deletedItem.setDeleted(true);
        // 创建 ItemDTO 对象
        ItemDTO deletedItemDto = deletedItemDTOs.computeIfAbsent(key, k -> new ItemDTO());
        deletedItemDto.setKey(key);
        String oldValue = entry.getValue();
        deletedItem.setItem(deletedItemDto);

        deletedItemDto.setValue(oldValue);
        deletedItem.setModified(true);
        deletedItem.setOldValue(oldValue);
        deletedItem.setNewValue("");
        // 添加到 `deletedItems` 中
        deletedItems.add(deletedItem);
      }
    }
    // 返回，已删除的 ItemBO 数组
    return deletedItems;
  }

  private ItemBO transformItem2BO(ItemDTO itemDTO, Map<String, String> releaseItems) {
    String key = itemDTO.getKey();
    // 创建 ItemBO 对象
    ItemBO itemBO = new ItemBO();
    itemBO.setItem(itemDTO);
    String newValue = itemDTO.getValue();
    String oldValue = releaseItems.get(key);
    //new item or modified
    if (!StringUtils.isEmpty(key) && (!newValue.equals(oldValue))) {
      itemBO.setModified(true);
      itemBO.setOldValue(oldValue == null ? "" : oldValue);
      itemBO.setNewValue(newValue);
    }
    return itemBO;
  }

  public void assignNamespaceRoleToOperator(String appId, String namespaceName, String operator) {
    //default assign modify、release namespace role to namespace creator

    rolePermissionService
        .assignRoleToUsers(
            RoleUtils.buildNamespaceRoleName(appId, namespaceName, RoleType.MODIFY_NAMESPACE),
            Sets.newHashSet(operator), operator);
    rolePermissionService
        .assignRoleToUsers(
            RoleUtils.buildNamespaceRoleName(appId, namespaceName, RoleType.RELEASE_NAMESPACE),
            Sets.newHashSet(operator), operator);
  }
}
