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

package org.apache.flume.sink.hive.batch.zk;

import org.apache.flume.sink.hive.batch.util.CommonUtils;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by Tao Li on 2/5/15.
 */
public class ZKService {
  private Logger LOG = LoggerFactory.getLogger(ZKService.class);

  private final static String ONLINE_PATH = "/online";

  private String zkURL;
  private String serviceName;
  private int sessionTimeout = 5000;
  private ZKClientWrapper client = null;

  private volatile ServerInfo currentServerInfo = new ServerInfo();

  private enum ZKState {
    STOPPED, // set only when call close() or exceed max connect retry times
    RUNNING,
    EXPIRED    // set only when zk received Expired event
  }

  private volatile ZKState state = ZKState.STOPPED;

  private BlockingQueue<ZKServiceOnStopCallBack> callBacks = new LinkedBlockingQueue<ZKServiceOnStopCallBack>();

  public ZKService(String zkURL, String serviceName, String hostName, int sessionTimeout) {
    this.zkURL = zkURL;
    this.serviceName = serviceName;
    this.sessionTimeout = sessionTimeout;
    this.currentServerInfo.hostName = hostName;
  }

  private class ZKClientWrapper {
    private ZooKeeper zk = null;
    private ZKWatcher watcher = null;

    // a CountDownLatch initialized with 1 and used to wait zk connection
    private CountDownLatch connectedSignal = new CountDownLatch(1);

    public ZKClientWrapper() throws IOException, InterruptedException, ZKServiceException {
      watcher = new ZKWatcher();
      zk = new ZooKeeper(zkURL, sessionTimeout, watcher);
      if (connectedSignal.await(10000, TimeUnit.MILLISECONDS)) {
        LOG.info("Zookeeper connection create succeed.");
      } else {
        LOG.error("Zookeeper connection create failed.");
        throw new ZKServiceException("Fail to create zookeeper connection.");
      }
    }

    public void close() throws InterruptedException {
      if (zk != null) {
        zk.close();
        LOG.info("Zookeeper connection closed.");
      }
    }

    private class ZKWatcher implements Watcher {
      @Override
      public void process(WatchedEvent event) {
        LOG.debug("Receive event: {}, {}", event.getType(), event.getState());
        if (event.getState() == Event.KeeperState.SyncConnected) {
          connectedSignal.countDown();
        } else if (event.getState() == Event.KeeperState.Expired && isRunning()) {
          synchronized (this) {
            if (isRunning()) {
              // Double check
              // if ZKService is STOPPED, no need to enter synchronized block
              // when enter synchronized block, if ZKService is STOPPED, no need to set EXPIRED
              state = ZKState.EXPIRED;
            }
          }
        }
      }
    }
  }

  private class ZKClientManagerThread implements Runnable {
    private long SLEEP_INTERVAL_SECONDS = 10;
    private String THREAD_NAME = "ZKClientManagerThread";
    private int MAX_CONNECT_RETRY_TIMES = 20;
    private int connectRetryTimes = 0;

    @Override
    public void run() {
      Thread.currentThread().setName(THREAD_NAME);

      while (isRunning()) {
        synchronized (this) {
          if (isRunning() && state == ZKState.EXPIRED) {
            LOG.info("ZooKeeper is expired.");
            try {
              offline();
              if (connectRetryTimes >= MAX_CONNECT_RETRY_TIMES) {
                state = ZKState.STOPPED;
              } else {

                connectRetryTimes++;
                LOG.info("Try to reconnect... (connectRetryTimes: {})", connectRetryTimes);
                online();
                state = ZKState.RUNNING;
                connectRetryTimes = 0;
              }
            } catch (Exception e) {
              LOG.error(CommonUtils.getStackTraceStr(e));
              if (client != null) {
                try {
                  client.close();
                } catch (InterruptedException e1) {
                  LOG.error(CommonUtils.getStackTraceStr(e));
                }
              }
            }
          }
        }

        try {
          TimeUnit.SECONDS.sleep(SLEEP_INTERVAL_SECONDS);
        } catch (InterruptedException e) {
          LOG.error(CommonUtils.getStackTraceStr(e));
        }
      }

      stopQuietly();
    }
  }

  public class ServerInfo {
    String hostName;
    long sessionId;
    String sequenceId;

    public ServerInfo() {

    }

    public ServerInfo(String hostName, long sessionId, String sequenceId) {
      this.hostName = hostName;
      this.sessionId = sessionId;
      this.sequenceId = sequenceId;
    }

    public String getHostName() {
      return hostName;
    }

    public String getZNodePath() {
      return String.format("%s_%s_%s", hostName, sessionId, sequenceId);
    }

    public String getFullZnodePath() {
      return getRootPath() + "/" + getZNodePath();
    }

    @Override
    public String toString() {
      return "ServerInfo{" +
          "hostName='" + hostName + '\'' +
          ", sessionId='" + sessionId + '\'' +
          ", sequenceId='" + sequenceId + '\'' +
          '}';
    }
  }

  public interface ZKServiceOnStopCallBack {
    void cleanUp();
  }

  public void start() throws ZKServiceException {
    synchronized (this) {
      if (isRunning()) {
        LOG.info("ZKService is already running.");
        return;
      }

      try {
        online();
        state = ZKState.RUNNING;
        new Thread(new ZKClientManagerThread()).start();
        LOG.info("ZKService is started.");
      } catch (Exception e) {
        LOG.error(CommonUtils.getStackTraceStr(e));
        stopQuietly();
        throw new ZKServiceException("Fail to start ZKService.", e);
      }
    }
  }

  private void create(String path, boolean isChild) throws KeeperException, InterruptedException {
    if (client.zk.exists(path, false) == null)
      client.zk.create(path, null,
          ZooDefs.Ids.OPEN_ACL_UNSAFE,
          isChild ? CreateMode.EPHEMERAL : CreateMode.PERSISTENT);
  }

  private void prepareEnv() throws KeeperException, InterruptedException {
    create("/" + this.serviceName, false);
    create(getRootPath(), false);
  }

  private void online()
      throws KeeperException, InterruptedException, IOException, ZKServiceException {
    client = new ZKClientWrapper();
    prepareEnv();
    currentServerInfo.sessionId = client.zk.getSessionId();
    String zNodeBasePath = String.format("%s/%s_%s_",
        getRootPath(), currentServerInfo.hostName, currentServerInfo.sessionId);
    String path = client.zk.create(zNodeBasePath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE,
        CreateMode.EPHEMERAL_SEQUENTIAL);
    String[] info = path.split("_");
    if (info.length == 3) {
      currentServerInfo.sequenceId = info[2];
    }
    client.zk.exists(currentServerInfo.getFullZnodePath(), true);

    LOG.info("Server {} online.", currentServerInfo.hostName);
  }

  public List<ServerInfo> getAllServerInfos() throws KeeperException, InterruptedException {
    // FIXME need to call sync() to get latest view
    List<ServerInfo> serverInfos = new ArrayList<ServerInfo>();
    for (String path : client.zk.getChildren(getRootPath(), false)) {
      String[] info = path.split("_");
      if (info.length == 3) {
        serverInfos.add(new ServerInfo(info[0], Long.parseLong(info[1]), info[2]));
      } else {
        LOG.error("Invalid server znode path: " + path);
      }
    }
    return serverInfos;
  }

  public ServerInfo getLeader() throws KeeperException, InterruptedException {
    List<ServerInfo> serverInfos = getAllServerInfos();
    ServerInfo leader = null;
    for (ServerInfo serverInfo : serverInfos) {
      if (leader == null) {
        leader = serverInfo;
      } else {
        if (serverInfo.sequenceId.compareTo(leader.sequenceId) < 0) {
          leader = serverInfo;
        }
      }
    }
    return leader;
  }

  public boolean isRunning() {
    return state != ZKState.STOPPED;
  }

  public void stop() throws ZKServiceException {
    synchronized (this) {
      state = ZKState.STOPPED;
      try {
        offline();
        LOG.info("ZKService is stopped.");
        executeAllCallBacks();
      } catch (Exception e) {
        LOG.error(CommonUtils.getStackTraceStr(e));
        throw new ZKServiceException("Fail to stop ZKService.", e);
      }
    }
  }

  public void stopQuietly() {
    try {
      stop();
    } catch (Exception e) {
      LOG.error(CommonUtils.getStackTraceStr(e));
    }
  }

  public void addCallBack(ZKServiceOnStopCallBack callBack) throws InterruptedException {
    callBacks.put(callBack);
  }

  private void executeAllCallBacks() throws InterruptedException {
    while (!callBacks.isEmpty()) {
      ZKServiceOnStopCallBack callBack = callBacks.take();
      try {
        LOG.info("execute callback on zk stopped ...");
        callBack.cleanUp();
      } catch (Exception e) {
        LOG.error(CommonUtils.getStackTraceStr(e));
      }
    }
  }

  private void offline() throws InterruptedException {
    if (client != null)
      client.close();
    LOG.info("Server {} offline.", currentServerInfo.hostName);
  }

  private String getRootPath() {
    return "/" + this.serviceName + ONLINE_PATH;
  }
}