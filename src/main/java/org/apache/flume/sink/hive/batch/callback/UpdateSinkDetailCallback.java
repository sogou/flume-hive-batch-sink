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

package org.apache.flume.sink.hive.batch.callback;

import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.hive.batch.HiveBatchWriter;
import org.apache.flume.sink.hive.batch.counter.TimedSinkCounter;
import org.apache.flume.sink.hive.batch.dao.HiveSinkDetailDao;
import org.apache.flume.sink.hive.batch.util.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * Created by Tao Li on 2016/3/2.
 */
public class UpdateSinkDetailCallback implements HiveBatchWriter.Callback {
  private Logger LOG = LoggerFactory.getLogger(UpdateSinkDetailCallback.class);

  private final String connectURL;
  private final String name;
  private final String logdate;
  private final String hostName;

  private TimedSinkCounter sinkCounter = null;

  public UpdateSinkDetailCallback(String connectURL, String name, String logdate, String hostName,
                                  SinkCounter sinkCounter) {
    this.connectURL = connectURL;
    this.name = name;
    this.logdate = logdate;
    this.hostName = hostName;
    if (sinkCounter instanceof TimedSinkCounter) {
      this.sinkCounter = (TimedSinkCounter) sinkCounter;
    }
  }

  @Override
  public void run() {
    try (HiveSinkDetailDao dao = new HiveSinkDetailDao(connectURL, name)) {
      dao.connect();
      long receiveCount = 0;
      long sinkCount = 0;
      if (sinkCounter != null &&
          sinkCounter.getEventDrainSuccessCountInFiveMinMap().containsKey(logdate)) {
        sinkCount = sinkCounter.getEventDrainSuccessCountInFiveMinMap().get(logdate).getCount();
      }
      long updateTimestamp = System.currentTimeMillis();
      if (dao.exists(logdate, hostName)) {
        dao.update(logdate, hostName, receiveCount, sinkCount, updateTimestamp);
      } else {
        dao.create(logdate, hostName, receiveCount, sinkCount, updateTimestamp);
      }
    } catch (SQLException e) {
      LOG.error(CommonUtils.getStackTraceStr(e));
    }
  }
}
