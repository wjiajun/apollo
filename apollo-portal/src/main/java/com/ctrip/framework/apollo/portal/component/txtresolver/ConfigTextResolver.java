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
package com.ctrip.framework.apollo.portal.component.txtresolver;

import com.ctrip.framework.apollo.common.dto.ItemChangeSets;
import com.ctrip.framework.apollo.common.dto.ItemDTO;

import java.util.List;

/**
 * users can modify config in text mode.so need resolve text.
 */
public interface ConfigTextResolver {

  /**
   * 解析文本，创建 ItemChangeSets 对象
   *
   * @param namespaceId Namespace 编号
   * @param configText 配置文本
   * @param baseItems 已存在的 ItemDTO 们
   * @return ItemChangeSets 对象
   */
  ItemChangeSets resolve(long namespaceId, String configText, List<ItemDTO> baseItems);

}
