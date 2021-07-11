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
package com.ctrip.framework.apollo.core.schedule;

/**
 * @author Jason Song(song_s@ctrip.com)
 *
 * 实现 SchedulePolicy 接口，基于指数级计算的定时策略实现类
 */
public class ExponentialSchedulePolicy implements SchedulePolicy {

  /**
   * 延迟时间下限
   */
  private final long delayTimeLowerBound;
  /**
   * 延迟时间上限
   */
  private final long delayTimeUpperBound;
  /**
   * 最后延迟执行时间
   */
  private long lastDelayTime;

  public ExponentialSchedulePolicy(long delayTimeLowerBound, long delayTimeUpperBound) {
    this.delayTimeLowerBound = delayTimeLowerBound;
    this.delayTimeUpperBound = delayTimeUpperBound;
  }

  @Override
  public long fail() {
    long delayTime = lastDelayTime;

    // 设置初始时间
    if (delayTime == 0) {
      delayTime = delayTimeLowerBound;
    } else {
      // 指数级计算，直到上限
      delayTime = Math.min(lastDelayTime << 1, delayTimeUpperBound);
    }

    // 最后延迟执行时间
    lastDelayTime = delayTime;

    // 返回
    return delayTime;
  }

  @Override
  public void success() {
    lastDelayTime = 0;
  }
}
