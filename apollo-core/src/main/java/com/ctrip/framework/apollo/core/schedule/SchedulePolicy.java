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
 * Schedule policy
 * @author Jason Song(song_s@ctrip.com)
 *
 * 定时策略接口。在 Apollo 中，用于执行失败，计算下一次执行的延迟时间
 */
public interface SchedulePolicy {
  /**
   * 执行失败
   *
   * @return 下次执行延迟
   */
  long fail();

  /**
   * 执行成功
   */
  void success();
}
