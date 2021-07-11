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
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.apollo.core.utils.StringUtils;

import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.List;

@Component("fileTextResolver")
public class FileTextResolver implements ConfigTextResolver {


  @Override
  public ItemChangeSets resolve(long namespaceId, String configText, List<ItemDTO> baseItems) {
    ItemChangeSets changeSets = new ItemChangeSets();
    // 配置文本为空，不进行修改
    if (CollectionUtils.isEmpty(baseItems) && StringUtils.isEmpty(configText)) {
      return changeSets;
    }
    // 不存在已有配置，创建 ItemDTO 到 ItemChangeSets 新增项
    if (CollectionUtils.isEmpty(baseItems)) {
      changeSets.addCreateItem(createItem(namespaceId, 0, configText));
    } else {
      // 已存在配置，创建 ItemDTO 到 ItemChangeSets 修改项
      ItemDTO beforeItem = baseItems.get(0);
      if (!configText.equals(beforeItem.getValue())) {//update
        changeSets.addUpdateItem(createItem(namespaceId, beforeItem.getId(), configText));
      }
    }

    return changeSets;
  }

  private ItemDTO createItem(long namespaceId, long itemId, String value) {
    ItemDTO item = new ItemDTO();
    item.setId(itemId);
    item.setNamespaceId(namespaceId);
    item.setValue(value);
    item.setLineNum(1);
    item.setKey(ConfigConsts.CONFIG_FILE_CONTENT_KEY);
    return item;
  }
}
