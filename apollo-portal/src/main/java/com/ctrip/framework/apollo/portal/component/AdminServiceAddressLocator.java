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
package com.ctrip.framework.apollo.portal.component;

import com.ctrip.framework.apollo.portal.environment.PortalMetaDomainService;
import com.ctrip.framework.apollo.core.dto.ServiceDTO;
import com.ctrip.framework.apollo.portal.environment.Env;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.http.HttpMessageConverters;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 在 apollo-portal 项目中，com.ctrip.framework.apollo.portal.component.AdminServiceAddressLocator ，Admin Service 定位器。
 *
 * 初始时，创建延迟 1 秒的任务，从 Meta Service 获取 Admin Service 集群地址进行缓存。
 * 获取成功时，创建延迟 5 分钟的任务，从 Meta Service 获取 Admin Service 集群地址刷新缓存。
 * 获取失败时，创建延迟 10 秒的任务，从 Meta Service 获取 Admin Service 集群地址刷新缓存。
 */
@Component
public class AdminServiceAddressLocator {

  private static final long NORMAL_REFRESH_INTERVAL = 5 * 60 * 1000;
  private static final long OFFLINE_REFRESH_INTERVAL = 10 * 1000;
  private static final int RETRY_TIMES = 3;
  private static final String ADMIN_SERVICE_URL_PATH = "/services/admin";
  private static final Logger logger = LoggerFactory.getLogger(AdminServiceAddressLocator.class);

  /**
   * 定时任务 ExecutorService
   */
  private ScheduledExecutorService refreshServiceAddressService;
  private RestTemplate restTemplate;
  /**
   * Env 数组
   */
  private List<Env> allEnvs;
  /**
   * List<ServiceDTO 缓存 Map
   *
   * KEY：ENV
   */
  private Map<Env, List<ServiceDTO>> cache = new ConcurrentHashMap<>();

  private final PortalSettings portalSettings;
  private final RestTemplateFactory restTemplateFactory;
  private final PortalMetaDomainService portalMetaDomainService;

  public AdminServiceAddressLocator(
      final HttpMessageConverters httpMessageConverters,
      final PortalSettings portalSettings,
      final RestTemplateFactory restTemplateFactory,
      final PortalMetaDomainService portalMetaDomainService
  ) {
    this.portalSettings = portalSettings;
    this.restTemplateFactory = restTemplateFactory;
    this.portalMetaDomainService = portalMetaDomainService;
  }

  @PostConstruct
  public void init() {
    // 获得 Env 数组
    allEnvs = portalSettings.getAllEnvs();

    //init restTemplate
    restTemplate = restTemplateFactory.getObject();

    // 创建 ScheduledExecutorService
    refreshServiceAddressService =
        Executors.newScheduledThreadPool(1, ApolloThreadFactory.create("ServiceLocator", true));

    // 创建延迟任务，1 秒后拉取 Admin Service 地址
    refreshServiceAddressService.schedule(new RefreshAdminServerAddressTask(), 1, TimeUnit.MILLISECONDS);
  }

  public List<ServiceDTO> getServiceList(Env env) {
    // 从缓存中获得 ServiceDTO 数组
    List<ServiceDTO> services = cache.get(env);
    // 若不存在，直接返回空数组。这点和 ConfigServiceLocator 不同。
    if (CollectionUtils.isEmpty(services)) {
      return Collections.emptyList();
    }
    // 打乱 ServiceDTO 数组，返回。实现 Client 级的负载均衡
    List<ServiceDTO> randomConfigServices = Lists.newArrayList(services);
    Collections.shuffle(randomConfigServices);
    return randomConfigServices;
  }

  //maintain admin server address
  private class RefreshAdminServerAddressTask implements Runnable {

    @Override
    public void run() {
      boolean refreshSuccess = true;
      // 循环多个 Env ，请求对应的 Meta Service ，获得 Admin Service 集群地址
      //refresh fail if get any env address fail
      for (Env env : allEnvs) {
        boolean currentEnvRefreshResult = refreshServerAddressCache(env);
        refreshSuccess = refreshSuccess && currentEnvRefreshResult;
      }

      // 若刷新成功，则创建定时任务，5 分钟后执行
      if (refreshSuccess) {
        refreshServiceAddressService
            .schedule(new RefreshAdminServerAddressTask(), NORMAL_REFRESH_INTERVAL, TimeUnit.MILLISECONDS);
      } else {
        // 若刷新失败，则创建定时任务，10 秒后执行
        refreshServiceAddressService
            .schedule(new RefreshAdminServerAddressTask(), OFFLINE_REFRESH_INTERVAL, TimeUnit.MILLISECONDS);
      }
    }
  }

  private boolean refreshServerAddressCache(Env env) {

    for (int i = 0; i < RETRY_TIMES; i++) {

      try {
        // 请求 Meta Service ，获得 Admin Service 集群地址
        ServiceDTO[] services = getAdminServerAddress(env);
        // 获得结果为空，continue ，继续执行下一次请求
        if (services == null || services.length == 0) {
          continue;
        }
        // 更新缓存
        cache.put(env, Arrays.asList(services));
        // 返回获取成功
        return true;
      } catch (Throwable e) {
        logger.error(String.format("Get admin server address from meta server failed. env: %s, meta server address:%s",
                                   env, portalMetaDomainService.getDomain(env)), e);
        Tracer
            .logError(String.format("Get admin server address from meta server failed. env: %s, meta server address:%s",
                                    env, portalMetaDomainService.getDomain(env)), e);
      }
    }
    return false;
  }

  private ServiceDTO[] getAdminServerAddress(Env env) {
    String domainName = portalMetaDomainService.getDomain(env);
    String url = domainName + ADMIN_SERVICE_URL_PATH;
    return restTemplate.getForObject(url, ServiceDTO[].class);
  }


}
