/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.sink.hive.batch.counter;

import org.apache.flume.Event;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.hive.batch.util.TimedUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Tao Li on 4/29/15.
 */
public class TimedSinkCounter extends SinkCounter implements TimedSinkCounterMBean {
  private Map<String, TimedUtils.TimestampCount> eventDrainSuccessCountInFiveMinMap =
      new TimedUtils.FiveMinLinkedHashMap();
  private Map<String, Map<String, TimedUtils.TimestampCount>> categoryEventDrainSuccessCountInFiveMinMap =
      new HashMap<>();

  private static final String COUNTER_EVENT_DRAIN_SUCCESS_IN_FIVE_MIN =
      "sink.event.drain.sucess.5min";
  private static final String COUNTER_CATEGORY_EVENT_DRAIN_SUCCESS_IN_FIVE_MIN =
      "sink.category.event.drain.sucess.5min";

  private static final String[] ATTRIBUTES =
      {
          COUNTER_EVENT_DRAIN_SUCCESS_IN_FIVE_MIN, COUNTER_CATEGORY_EVENT_DRAIN_SUCCESS_IN_FIVE_MIN
      };

  public TimedSinkCounter(String name) {
    super(name, ATTRIBUTES);
  }

  public void addToEventDrainSuccessCountInFiveMinMap(long delta) {
    TimedUtils.updateFiveMinMap(delta, eventDrainSuccessCountInFiveMinMap);
  }

  @Override
  public String getEventDrainSuccessCountInFiveMinJson() {
    return TimedUtils.convertFiveMinMapToJson(eventDrainSuccessCountInFiveMinMap);
  }

  public Map<String, TimedUtils.TimestampCount> getEventDrainSuccessCountInFiveMinMap() {
    return eventDrainSuccessCountInFiveMinMap;
  }

  public void addToCategoryEventDrainSuccessCountInFiveMinMap(List<Event> events,
                                                              String categoryKey) {
    TimedUtils.updateCategoryFiveMinMap(events, categoryEventDrainSuccessCountInFiveMinMap,
        categoryKey);
  }

  public void addToCategoryEventDrainSuccessCountInFiveMinMap(List<Event> events) {
    TimedUtils.updateCategoryFiveMinMap(events, categoryEventDrainSuccessCountInFiveMinMap);
  }

  @Override
  public String getCategoryEventDrainSuccessCountInFiveMinJson() {
    return TimedUtils.convertCategoryFiveMinMapToJson(categoryEventDrainSuccessCountInFiveMinMap);
  }
}