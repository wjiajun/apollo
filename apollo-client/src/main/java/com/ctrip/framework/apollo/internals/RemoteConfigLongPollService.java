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

import com.ctrip.framework.apollo.build.ApolloInjector;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.apollo.core.dto.ApolloConfigNotification;
import com.ctrip.framework.apollo.core.dto.ApolloNotificationMessages;
import com.ctrip.framework.apollo.core.dto.ServiceDTO;
import com.ctrip.framework.apollo.core.enums.ConfigFileFormat;
import com.ctrip.framework.apollo.core.schedule.ExponentialSchedulePolicy;
import com.ctrip.framework.apollo.core.schedule.SchedulePolicy;
import com.ctrip.framework.apollo.core.signature.Signature;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.core.utils.StringUtils;
import com.ctrip.framework.apollo.exceptions.ApolloConfigException;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;
import com.ctrip.framework.apollo.util.ConfigUtil;
import com.ctrip.framework.apollo.util.ExceptionUtil;
import com.ctrip.framework.apollo.util.http.HttpRequest;
import com.ctrip.framework.apollo.util.http.HttpResponse;
import com.ctrip.framework.apollo.util.http.HttpClient;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.escape.Escaper;
import com.google.common.net.UrlEscapers;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jason Song(song_s@ctrip.com)
 *
 * 远程配置长轮询服务 负责长轮询 Config Service 的配置变更通知 /notifications/v2 接口。
 * 当有新的通知时，触发 RemoteConfigRepository ，立即轮询 Config Service 的配置读取 /configs/{appId}/{clusterName}/{namespace:.+} 接口。
 *
 * 一个 Namespace 对应一个 RemoteConfigRepository 。
 * 多个 RemoteConfigRepository ，注册到全局唯一的 RemoteConfigLongPollService 中。
 */
public class RemoteConfigLongPollService {
  private static final Logger logger = LoggerFactory.getLogger(RemoteConfigLongPollService.class);
  private static final Joiner STRING_JOINER = Joiner.on(ConfigConsts.CLUSTER_NAMESPACE_SEPARATOR);
  private static final Joiner.MapJoiner MAP_JOINER = Joiner.on("&").withKeyValueSeparator("=");
  private static final Escaper queryParamEscaper = UrlEscapers.urlFormParameterEscaper();
  private static final long INIT_NOTIFICATION_ID = ConfigConsts.NOTIFICATION_ID_PLACEHOLDER;
  //90 seconds, should be longer than server side's long polling timeout, which is now 60 seconds
  private static final int LONG_POLLING_READ_TIMEOUT = 90 * 1000;
  /**
   * 长轮询 ExecutorService
   */
  private final ExecutorService m_longPollingService;

  /**
   * 是否停止长轮询的标识
   */
  private final AtomicBoolean m_longPollingStopped;

  /**
   * 失败定时重试策略，使用 {@link ExponentialSchedulePolicy}
   */
  private SchedulePolicy m_longPollFailSchedulePolicyInSecond;

  /**
   * 长轮询的 RateLimiter
   */
  private RateLimiter m_longPollRateLimiter;

  /**
   * 是否长轮询已经开始的标识
   */
  private final AtomicBoolean m_longPollStarted;

  /**
   * 长轮询的 Namespace Multimap 缓存
   *
   * 通过 {@link #submit(String, RemoteConfigRepository)} 添加 RemoteConfigRepository 。
   *
   * KEY：Namespace 的名字
   * VALUE：RemoteConfigRepository 集合
   */
  private final Multimap<String, RemoteConfigRepository> m_longPollNamespaces;
  /**
   * 通知编号 Map 缓存
   *
   * KEY：Namespace 的名字
   * VALUE：最新的通知编号
   */
  private final ConcurrentMap<String, Long> m_notifications;
  /**
   * 通知消息 Map 缓存
   *
   * KEY：Namespace 的名字
   * VALUE：ApolloNotificationMessages 对象
   */
  private final Map<String, ApolloNotificationMessages> m_remoteNotificationMessages;//namespaceName -> watchedKey -> notificationId
  private Type m_responseType;
  private static final Gson GSON = new Gson();
  private ConfigUtil m_configUtil;
  private HttpClient m_httpClient;
  private ConfigServiceLocator m_serviceLocator;

  /**
   * Constructor.
   */
  public RemoteConfigLongPollService() {
    m_longPollFailSchedulePolicyInSecond = new ExponentialSchedulePolicy(1, 120); //in second
    m_longPollingStopped = new AtomicBoolean(false);
    m_longPollingService = Executors.newSingleThreadExecutor(
        ApolloThreadFactory.create("RemoteConfigLongPollService", true));
    m_longPollStarted = new AtomicBoolean(false);
    m_longPollNamespaces =
        Multimaps.synchronizedSetMultimap(HashMultimap.<String, RemoteConfigRepository>create());
    m_notifications = Maps.newConcurrentMap();
    m_remoteNotificationMessages = Maps.newConcurrentMap();
    m_responseType = new TypeToken<List<ApolloConfigNotification>>() {
    }.getType();
    m_configUtil = ApolloInjector.getInstance(ConfigUtil.class);
    m_httpClient = ApolloInjector.getInstance(HttpClient.class);
    m_serviceLocator = ApolloInjector.getInstance(ConfigServiceLocator.class);
    m_longPollRateLimiter = RateLimiter.create(m_configUtil.getLongPollQPS());
  }

  /**
   * 提交 RemoteConfigRepository 到长轮询任务
   *
   * @param namespace
   * @param remoteConfigRepository
   * @return
   */
  public boolean submit(String namespace, RemoteConfigRepository remoteConfigRepository) {
    // 添加到 m_longPollNamespaces 中
    boolean added = m_longPollNamespaces.put(namespace, remoteConfigRepository);
    // 添加到 m_notifications 中
    m_notifications.putIfAbsent(namespace, INIT_NOTIFICATION_ID);
    // 若未启动长轮询定时任务，进行启动
    if (!m_longPollStarted.get()) {
      startLongPolling();
    }
    return added;
  }

  /**
   * 启动长轮询任务
   */
  private void startLongPolling() {
    // CAS 设置长轮询任务已经启动。若已经启动，不重复启动。
    if (!m_longPollStarted.compareAndSet(false, true)) {
      //already started
      return;
    }
    try {
      // 获得 appId cluster dataCenter 配置信息
      final String appId = m_configUtil.getAppId();
      final String cluster = m_configUtil.getCluster();
      final String dataCenter = m_configUtil.getDataCenter();
      final String secret = m_configUtil.getAccessKeySecret();
      // 获得长轮询任务的初始化延迟时间，单位毫秒。默认，2000 毫秒。
      final long longPollingInitialDelayInMills = m_configUtil.getLongPollingInitialDelayInMills();
      // 提交长轮询任务。该任务会持续且循环执行。
      m_longPollingService.submit(new Runnable() {
        @Override
        public void run() {
          // 初始等待
          if (longPollingInitialDelayInMills > 0) {
            try {
              logger.debug("Long polling will start in {} ms.", longPollingInitialDelayInMills);
              TimeUnit.MILLISECONDS.sleep(longPollingInitialDelayInMills);
            } catch (InterruptedException e) {
              //ignore
            }
          }
          // 执行长轮询
          doLongPollingRefresh(appId, cluster, dataCenter, secret);
        }
      });
    } catch (Throwable ex) {
      // 设置 m_longPollStarted 为 false
      m_longPollStarted.set(false);
      ApolloConfigException exception =
          new ApolloConfigException("Schedule long polling refresh failed", ex);
      Tracer.logError(exception);
      logger.warn(ExceptionUtil.getDetailMessage(exception));
    }
  }

  void stopLongPollingRefresh() {
    this.m_longPollingStopped.compareAndSet(false, true);
  }

  /**
   * 持续执行长轮询
   *
   * @param appId
   * @param cluster
   * @param dataCenter
   * @param secret
   */
  private void doLongPollingRefresh(String appId, String cluster, String dataCenter, String secret) {
    final Random random = new Random();
    ServiceDTO lastServiceDto = null;
    // 循环执行，直到停止或线程中断
    while (!m_longPollingStopped.get() && !Thread.currentThread().isInterrupted()) {
      // 限流
      if (!m_longPollRateLimiter.tryAcquire(5, TimeUnit.SECONDS)) {
        //wait at most 5 seconds
        try {
          TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
        }
      }
      Transaction transaction = Tracer.newTransaction("Apollo.ConfigService", "pollNotification");
      String url = null;
      try {
        // 获得 Config Service 的地址
        if (lastServiceDto == null) {
          // 获得所有的 Config Service 的地址
          List<ServiceDTO> configServices = getConfigServices();
          lastServiceDto = configServices.get(random.nextInt(configServices.size()));
        }

        // 组装长轮询通知变更的地址
        url =
            assembleLongPollRefreshUrl(lastServiceDto.getHomepageUrl(), appId, cluster, dataCenter,
                m_notifications);

        logger.debug("Long polling from {}", url);

        // 创建 HttpRequest 对象，并设置超时时间
        HttpRequest request = new HttpRequest(url);
        // 默认超时时间为 90 秒，大于 Config Service 的通知接口的 60 秒。
        request.setReadTimeout(LONG_POLLING_READ_TIMEOUT);
        if (!StringUtils.isBlank(secret)) {
          Map<String, String> headers = Signature.buildHttpHeaders(url, appId, secret);
          request.setHeaders(headers);
        }

        transaction.addData("Url", url);

        // 发起请求，返回 HttpResponse 对象
        final HttpResponse<List<ApolloConfigNotification>> response =
            m_httpClient.doGet(request, m_responseType);

        logger.debug("Long polling response: {}, url: {}", response.getStatusCode(), url);
        // 有新的通知，刷新本地的缓存
        if (response.getStatusCode() == 200 && response.getBody() != null) {
          // 更新 m_notifications
          updateNotifications(response.getBody());
          // 更新 m_remoteNotificationMessages
          updateRemoteNotifications(response.getBody());
          transaction.addData("Result", response.getBody().toString());
          // 长轮询到该 RemoteConfigRepository 的 Namespace 下的配置更新时，会回调 #onLongPollNotified(ServiceDTO, ApolloNotificationMessages) 方法
          // 通知对应的 RemoteConfigRepository 们
          notify(lastServiceDto, response.getBody());
        }

        // 无新的通知，重置连接的 Config Service 的地址，下次请求不同的 Config Service ，实现负载均衡。
        //try to load balance
        if (response.getStatusCode() == 304 && random.nextBoolean()) {
          lastServiceDto = null;
        }

        // 标记成功
        m_longPollFailSchedulePolicyInSecond.success();
        transaction.addData("StatusCode", response.getStatusCode());
        transaction.setStatus(Transaction.SUCCESS);
      } catch (Throwable ex) {
        // 重置连接的 Config Service 的地址，下次请求不同的 Config Service
        lastServiceDto = null;
        Tracer.logEvent("ApolloConfigException", ExceptionUtil.getDetailMessage(ex));
        transaction.setStatus(ex);
        // 标记失败，计算下一次延迟执行时间
        long sleepTimeInSecond = m_longPollFailSchedulePolicyInSecond.fail();
        logger.warn(
            "Long polling failed, will retry in {} seconds. appId: {}, cluster: {}, namespaces: {}, long polling url: {}, reason: {}",
            sleepTimeInSecond, appId, cluster, assembleNamespaces(), url, ExceptionUtil.getDetailMessage(ex));
        // 等待一定时间，下次失败重试
        try {
          TimeUnit.SECONDS.sleep(sleepTimeInSecond);
        } catch (InterruptedException ie) {
          //ignore
        }
      } finally {
        transaction.complete();
      }
    }
  }

  private void notify(ServiceDTO lastServiceDto, List<ApolloConfigNotification> notifications) {
    if (notifications == null || notifications.isEmpty()) {
      return;
    }
    // 循环 ApolloConfigNotification
    for (ApolloConfigNotification notification : notifications) {
      String namespaceName = notification.getNamespaceName();
      //create a new list to avoid ConcurrentModificationException
      // 创建 RemoteConfigRepository 数组，避免并发问题
      List<RemoteConfigRepository> toBeNotified =
          Lists.newArrayList(m_longPollNamespaces.get(namespaceName));
      // 获得远程的 ApolloNotificationMessages 对象，并克隆
      ApolloNotificationMessages originalMessages = m_remoteNotificationMessages.get(namespaceName);
      ApolloNotificationMessages remoteMessages = originalMessages == null ? null : originalMessages.clone();
      //since .properties are filtered out by default, so we need to check if there is any listener for it
      // 因为 .properties 在默认情况下被过滤掉，所以我们需要检查是否有监听器。若有，添加到 RemoteConfigRepository 数组
      toBeNotified.addAll(m_longPollNamespaces
          .get(String.format("%s.%s", namespaceName, ConfigFileFormat.Properties.getValue())));
      // 循环 RemoteConfigRepository ，进行通知
      for (RemoteConfigRepository remoteConfigRepository : toBeNotified) {
        try {
          // 进行通知
          remoteConfigRepository.onLongPollNotified(lastServiceDto, remoteMessages);
        } catch (Throwable ex) {
          Tracer.logError(ex);
        }
      }
    }
  }

  private void updateNotifications(List<ApolloConfigNotification> deltaNotifications) {
    // 循环 ApolloConfigNotification
    for (ApolloConfigNotification notification : deltaNotifications) {
      if (Strings.isNullOrEmpty(notification.getNamespaceName())) {
        continue;
      }
      // 更新 m_notifications
      String namespaceName = notification.getNamespaceName();
      if (m_notifications.containsKey(namespaceName)) {
        m_notifications.put(namespaceName, notification.getNotificationId());
      }
      // 因为 .properties 在默认情况下被过滤掉，所以我们需要检查是否有 .properties 后缀的通知。如有，更新 m_notifications
      //since .properties are filtered out by default, so we need to check if there is notification with .properties suffix
      String namespaceNameWithPropertiesSuffix =
          String.format("%s.%s", namespaceName, ConfigFileFormat.Properties.getValue());
      if (m_notifications.containsKey(namespaceNameWithPropertiesSuffix)) {
        m_notifications.put(namespaceNameWithPropertiesSuffix, notification.getNotificationId());
      }
    }
  }

  private void updateRemoteNotifications(List<ApolloConfigNotification> deltaNotifications) {
    // 循环 ApolloConfigNotification
    for (ApolloConfigNotification notification : deltaNotifications) {
      if (Strings.isNullOrEmpty(notification.getNamespaceName())) {
        continue;
      }

      if (notification.getMessages() == null || notification.getMessages().isEmpty()) {
        continue;
      }

      // 若不存在 Namespace 对应的 ApolloNotificationMessages ，进行创建
      ApolloNotificationMessages localRemoteMessages =
          m_remoteNotificationMessages.get(notification.getNamespaceName());
      if (localRemoteMessages == null) {
        localRemoteMessages = new ApolloNotificationMessages();
        m_remoteNotificationMessages.put(notification.getNamespaceName(), localRemoteMessages);
      }

      // 合并通知消息到 ApolloNotificationMessages 中
      localRemoteMessages.mergeFrom(notification.getMessages());
    }
  }

  private String assembleNamespaces() {
    return STRING_JOINER.join(m_longPollNamespaces.keySet());
  }

  /**
   * 长轮询 Config Service 的配置变更通知 /notifications/v2 接口的 URL
   *
   * @param uri
   * @param appId
   * @param cluster
   * @param dataCenter
   * @param notificationsMap
   * @return
   */
  String assembleLongPollRefreshUrl(String uri, String appId, String cluster, String dataCenter,
                                    Map<String, Long> notificationsMap) {
    Map<String, String> queryParams = Maps.newHashMap();
    queryParams.put("appId", queryParamEscaper.escape(appId));
    queryParams.put("cluster", queryParamEscaper.escape(cluster));
    queryParams
        .put("notifications", queryParamEscaper.escape(assembleNotifications(notificationsMap)));

    if (!Strings.isNullOrEmpty(dataCenter)) {
      queryParams.put("dataCenter", queryParamEscaper.escape(dataCenter));
    }
    String localIp = m_configUtil.getLocalIp();
    if (!Strings.isNullOrEmpty(localIp)) {
      queryParams.put("ip", queryParamEscaper.escape(localIp));
    }
    // 创建 Query String
    String params = MAP_JOINER.join(queryParams);
    // 拼接 URL
    if (!uri.endsWith("/")) {
      uri += "/";
    }

    return uri + "notifications/v2?" + params;
  }

  String assembleNotifications(Map<String, Long> notificationsMap) {
    // 创建 ApolloConfigNotification 数组
    List<ApolloConfigNotification> notifications = Lists.newArrayList();
    // 循环，添加 ApolloConfigNotification 对象
    for (Map.Entry<String, Long> entry : notificationsMap.entrySet()) {
      ApolloConfigNotification notification = new ApolloConfigNotification(entry.getKey(), entry.getValue());
      notifications.add(notification);
    }
    // JSON 化成字符串
    return GSON.toJson(notifications);
  }

  private List<ServiceDTO> getConfigServices() {
    List<ServiceDTO> services = m_serviceLocator.getConfigServices();
    if (services.size() == 0) {
      throw new ApolloConfigException("No available config service");
    }

    return services;
  }
}
