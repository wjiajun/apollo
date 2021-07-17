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

import com.ctrip.framework.apollo.common.entity.App;

public interface RoleInitializationService {

  /**
   * 初始化 App 级的 Role
   */
  void initAppRoles(App app);

  /**
   * 初始化 Namespace 级的 Role
   */
  void initNamespaceRoles(String appId, String namespaceName, String operator);

  void initNamespaceEnvRoles(String appId, String namespaceName, String operator);

  void initNamespaceSpecificEnvRoles(String appId, String namespaceName, String env,
      String operator);

  void initCreateAppRole();

  void initManageAppMasterRole(String appId, String operator);

}
