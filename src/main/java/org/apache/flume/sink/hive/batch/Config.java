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

package org.apache.flume.sink.hive.batch;

/**
 * Created by Tao Li on 2016/2/17.
 */
public class Config {
  public static final String HIVE_DATABASE = "hive.database";
  public static final String HIVE_TABLE = "hive.table";
  public static final String HIVE_PATH = "hive.path";
  public static final String HIVE_PARTITION = "hive.partition";
  public static final String HIVE_FILE_PREFIX = "hive.filePrefix";
  public static final String HIVE_FILE_SUFFIX = "hive.fileSuffix";
  public static final String HIVE_TIME_ZONE = "hive.timeZone";
  public static final String HIVE_MAX_OPEN_FILES = "hive.maxOpenFiles";
  public static final String HIVE_BATCH_SIZE = "hive.batchSize";
  public static final String HIVE_IDLE_TIMEOUT = "hive.idleTimeout";
  public static final String HIVE_IDLE_QUEUE_SIZE = "hive.idleQueueSize";
  public static final String HIVE_IDLE_CLOSE_THREAD_POOL_SIZE = "hive.idleCloseThreadPoolSize";
  public static final String HIVE_SERDE = "hive.serde";
  public static final String HIVE_SERDE_PROPERTIES = "hive.serdeProperties";
  public static final String HIVE_ROUND = "hive.round";
  public static final String HIVE_ROUND_UNIT = "hive.roundUnit";
  public static final String HIVE_ROUND_VALUE = "hive.roundValue";
  public static final String HIVE_USE_LOCAL_TIMESTAMP = "hive.useLocalTimeStamp";
  public static final String HIVE_SINK_COUNTER_TYPE = "hive.sinkCounterType";
  public static final String Hive_ZOOKEEPER_CONNECT = "hive.zookeeperConnect";
  public static final String HIVE_ZOOKEEPER_SESSION_TIMEOUT = "hive.zookeeperSessionTimeout";
  public static final String HIVE_ZOOKEEPER_SERVICE_NAME = "hive.zookeeperServiceName";
  public static final String HIVE_HOST_NAME = "hive.hostName";
  public static final String HIVE_DB_CONNECT_URL = "hive.dbConnectURL";
  public static final String HIVE_DTE_UPDATE_LOGDETAIL_URL = "hive.dte.updateLogDetailURL";
  public static final String HIVE_DTE_LOGID = "hive.dte.logid";
  public static final String HIVE_DTE_LOGDATE_FORMAT = "hive.dte.logdateFormat";

  public static class Default {
    public static final String DEFAULT_DATABASE = "default";
    public static final String DEFAULT_PARTITION = "";
    public static final String DEFAULT_FILE_PREFIX = "FlumeData";
    public static final String DEFAULT_FILE_SUFFIX = "orc";
    public static final int DEFAULT_MAX_OPEN_FILES = 5000;
    public static final int DEFAULT_BATCH_SIZE = 1000;
    public static final long DEFAULT_IDLE_TIMEOUT = 5000;
    public static final int DEFAULT_IDLE_QUEUE_SZIE = 100;
    public static final int DEFAULT_IDLE_CLOSE_THREAD_POOL_SIZE = 10;
    public static final boolean DEFAULT_ROUND = false;
    public static final String DEFAULT_ROUND_UNIT = "second";
    public static final int DEFAULT_ROUND_VALUE = 1;
    public static final boolean DEFAULT_USE_LOCAL_TIMESTAMP = false;
    public static final String DEFAULT_SINK_COUNTER_TYPE = "SinkCounter";
    public static final String DEFAULT_ZOOKEEPER_CONNECT = null;
    public static final int DEFAULT_ZOOKEEPER_SESSION_TIMEOUT = 5000;
    public static final String DEFAULT_DB_CONNECT_URL = null;
    public static final String DEFAULT_DTE_LOGDATE_FORMAT = "yyyyMMddHHmm";
  }
}