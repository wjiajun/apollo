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

import com.google.common.collect.Maps;

import com.ctrip.framework.apollo.common.config.RefreshablePropertySource;
import com.ctrip.framework.apollo.portal.entity.po.ServerConfig;
import com.ctrip.framework.apollo.portal.repository.ServerConfigRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;


/**
 * @author Jason Song(song_s@ctrip.com)
 *
 * 实现 RefreshablePropertySource 抽象类，基于 PortalDB 的 ServerConfig 的 PropertySource 实现类
 */
@Component
public class PortalDBPropertySource extends RefreshablePropertySource {
  private static final Logger logger = LoggerFactory.getLogger(PortalDBPropertySource.class);

  @Autowired
  private ServerConfigRepository serverConfigRepository;

  public PortalDBPropertySource(String name, Map<String, Object> source) {
    super(name, source);
  }

  public PortalDBPropertySource() {
    super("DBConfig", Maps.newConcurrentMap());
  }

  @Override
  protected void refresh() {
    // 获得所有的 ServerConfig 记录
    Iterable<ServerConfig> dbConfigs = serverConfigRepository.findAll();

    // 缓存，更新到属性源
    for (ServerConfig config: dbConfigs) {
      String key = config.getKey();
      Object value = config.getValue();

      if (this.source.isEmpty()) {
        logger.info("Load config from DB : {} = {}", key, value);
      } else if (!Objects.equals(this.source.get(key), value)) {
        logger.info("Load config from DB : {} = {}. Old value = {}", key,
                    value, this.source.get(key));
      }

      // 更新到属性源
      this.source.put(key, value);
    }
  }


}
