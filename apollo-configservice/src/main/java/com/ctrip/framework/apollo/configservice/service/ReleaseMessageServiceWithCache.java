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
import com.ctrip.framework.apollo.biz.entity.ReleaseMessage;
import com.ctrip.framework.apollo.biz.message.ReleaseMessageListener;
import com.ctrip.framework.apollo.biz.message.Topics;
import com.ctrip.framework.apollo.biz.repository.ReleaseMessageRepository;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Jason Song(song_s@ctrip.com)
 *
 * 通过将 ReleaseMessage 缓存在内存中，提高查询性能
 */
@Service
public class ReleaseMessageServiceWithCache implements ReleaseMessageListener, InitializingBean {
  private static final Logger logger = LoggerFactory.getLogger(ReleaseMessageServiceWithCache
      .class);
  private final ReleaseMessageRepository releaseMessageRepository;
  private final BizConfig bizConfig;

  /**
   * 扫描周期
   */
  private int scanInterval;
  /**
   * 扫描周期单位
   */
  private TimeUnit scanIntervalTimeUnit;

  /**
   * 最后扫描到的 ReleaseMessage 的编号
   */
  private volatile long maxIdScanned;

  /**
   * ReleaseMessage 缓存
   *
   * KEY：`ReleaseMessage.message`
   * VALUE：对应的最新的 ReleaseMessage 记录
   */
  private ConcurrentMap<String, ReleaseMessage> releaseMessageCache;

  /**
   * 是否执行扫描任务
   */
  private AtomicBoolean doScan;
  /**
   * ExecutorService 对象
   */
  private ExecutorService executorService;

  public ReleaseMessageServiceWithCache(
      final ReleaseMessageRepository releaseMessageRepository,
      final BizConfig bizConfig) {
    this.releaseMessageRepository = releaseMessageRepository;
    this.bizConfig = bizConfig;
    initialize();
  }

  private void initialize() {
    // 创建缓存对象
    releaseMessageCache = Maps.newConcurrentMap();
    // 设置 doScan 为 true
    doScan = new AtomicBoolean(true);
    // 创建 ScheduledExecutorService 对象，大小为 1 。
    executorService = Executors.newSingleThreadExecutor(ApolloThreadFactory
        .create("ReleaseMessageServiceWithCache", true));
  }

  public ReleaseMessage findLatestReleaseMessageForMessages(Set<String> messages) {
    if (CollectionUtils.isEmpty(messages)) {
      return null;
    }

    long maxReleaseMessageId = 0;
    ReleaseMessage result = null;
    for (String message : messages) {
      ReleaseMessage releaseMessage = releaseMessageCache.get(message);
      if (releaseMessage != null && releaseMessage.getId() > maxReleaseMessageId) {
        maxReleaseMessageId = releaseMessage.getId();
        result = releaseMessage;
      }
    }

    return result;
  }

  /**
   * 获得每条消息内容对应的最新的 ReleaseMessage 对象
   *
   * @param messages 消息内容的集合
   * @return 集合
   */
  public List<ReleaseMessage> findLatestReleaseMessagesGroupByMessages(Set<String> messages) {
    if (CollectionUtils.isEmpty(messages)) {
      return Collections.emptyList();
    }
    List<ReleaseMessage> releaseMessages = Lists.newArrayList();

    for (String message : messages) {
      // 获得每条消息内容对应的最新的 ReleaseMessage 对象
      ReleaseMessage releaseMessage = releaseMessageCache.get(message);
      if (releaseMessage != null) {
        releaseMessages.add(releaseMessage);
      }
    }

    return releaseMessages;
  }

  @Override
  public void handleMessage(ReleaseMessage message, String channel) {
    //Could stop once the ReleaseMessageScanner starts to work
    // 关闭增量拉取定时任务的执行
    // 由本方法的最后一行loadReleaseMessages自行处理ReleaseMessage的变更
    doScan.set(false);
    logger.info("message received - channel: {}, message: {}", channel, message);

    // 仅处理 APOLLO_RELEASE_TOPIC
    String content = message.getMessage();
    Tracer.logEvent("Apollo.ReleaseMessageService.UpdateCache", String.valueOf(message.getId()));
    if (!Topics.APOLLO_RELEASE_TOPIC.equals(channel) || Strings.isNullOrEmpty(content)) {
      return;
    }

    // 计算 gap
    long gap = message.getId() - maxIdScanned;
    // 若无空缺 gap ，直接合并
    if (gap == 1) {
      mergeReleaseMessage(message);
    } else if (gap > 1) {
      // 如有空缺 gap ，增量拉取
      //gap found!
      // 增量拉取定时任务还来不及拉取( 即未执行 )，ReleaseMessageScanner 就已经通知
      // 注意：和定时任务不同的是：定时任务主要是用于处理启动过程中，release message新产生但程序还没有启动完成导致handle message无法执行的所遗漏的Release Message
      //      而这类主要是处理后续非启动过程中定时任务还没拉取而Scanner先回调导致ReleaseMessage遗漏的问题
      //        按照gap > 1的处理如果启动后还有ReleaseMessage 发布的话其实增强拉取定时任务可以不用开启，但是这不能保证，可能程序启动后再也没有ReleaseMessage发布，
      //        那么通过增量定时任务的处理能防止这个问题（启动后再无变动Message），如果有Message Release的话会关闭增量拉取定时任务的执行。
      loadReleaseMessages(maxIdScanned);
    }
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    // 从 ServerConfig 中，读取任务的周期配置
    populateDataBaseInterval();
    //block the startup process until load finished
    //this should happen before ReleaseMessageScanner due to autowire
    // 初始拉取 ReleaseMessage 到缓存
    loadReleaseMessages(0);

    // 创建定时任务，增量拉取 ReleaseMessage 到缓存，用以处理初始化期间，产生的 ReleaseMessage 遗漏的问题。
    // 20:00:00 程序启动过程中，当前 release message 有 5 条
    // 20:00:01 loadReleaseMessages(0); 执行完成，获取到 5 条记录
    // 20:00:02 有一条 release message 新产生，但是因为程序还没启动完，所以不会触发 handle message 操作
    // 20:00:05 程序启动完成，但是第三步的这条新的 release message 漏了
    // 20:10:00 假设这时又有一条 release message 产生，这次会触发 handle message ，同时会把第三步的那条 release message 加载到
    // 所以，定期刷的机制就是为了解决第三步中产生的release message问题。
    // 当程序启动完，handleMessage生效后，就不需要再定期扫了
    executorService.submit(() -> {
      while (doScan.get() && !Thread.currentThread().isInterrupted()) {
        Transaction transaction = Tracer.newTransaction("Apollo.ReleaseMessageServiceWithCache",
            "scanNewReleaseMessages");
        try {
          // 增量拉取 ReleaseMessage 到缓存
          loadReleaseMessages(maxIdScanned);
          transaction.setStatus(Transaction.SUCCESS);
        } catch (Throwable ex) {
          transaction.setStatus(ex);
          logger.error("Scan new release messages failed", ex);
        } finally {
          transaction.complete();
        }
        try {
          scanIntervalTimeUnit.sleep(scanInterval);
        } catch (InterruptedException e) {
          //ignore
        }
      }
    });
  }

  private synchronized void mergeReleaseMessage(ReleaseMessage releaseMessage) {
    // 获得对应的 ReleaseMessage 对象
    ReleaseMessage old = releaseMessageCache.get(releaseMessage.getMessage());
    // 若编号更大，进行更新缓存/ 若编号更大，进行更新缓存
    if (old == null || releaseMessage.getId() > old.getId()) {
      releaseMessageCache.put(releaseMessage.getMessage(), releaseMessage);
      maxIdScanned = releaseMessage.getId();
    }
  }

  private void loadReleaseMessages(long startId) {
    boolean hasMore = true;
    while (hasMore && !Thread.currentThread().isInterrupted()) {
      //current batch is 500
      // 获得大于 maxIdScanned 的 500 条 ReleaseMessage 记录，按照 id 升序
      List<ReleaseMessage> releaseMessages = releaseMessageRepository
          .findFirst500ByIdGreaterThanOrderByIdAsc(startId);
      if (CollectionUtils.isEmpty(releaseMessages)) {
        break;
      }
      // 合并到 ReleaseMessage 缓存
      releaseMessages.forEach(this::mergeReleaseMessage);
      int scanned = releaseMessages.size();
      // 获得新的 maxIdScanned ，取最后一条记录
      startId = releaseMessages.get(scanned - 1).getId();
      hasMore = scanned == 500;
      logger.info("Loaded {} release messages with startId {}", scanned, startId);
    }
  }

  private void populateDataBaseInterval() {
    // "apollo.release-message-cache-scan.interval" ，默认为 1
    scanInterval = bizConfig.releaseMessageCacheScanInterval();
    // 默认秒，不可配置。
    scanIntervalTimeUnit = bizConfig.releaseMessageCacheScanIntervalTimeUnit();
  }

  //only for test use
  private void reset() throws Exception {
    executorService.shutdownNow();
    initialize();
    afterPropertiesSet();
  }
}
