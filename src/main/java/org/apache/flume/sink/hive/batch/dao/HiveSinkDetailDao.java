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

package org.apache.flume.sink.hive.batch.dao;

import com.google.common.base.Joiner;
import org.apache.flume.sink.hive.batch.util.DBManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Tao Li on 2016/3/2.
 */
public class HiveSinkDetailDao {
  private Logger LOG = LoggerFactory.getLogger(HiveSinkDetailDao.class);

  private final String TABLE_NAME = "hive_sink_detail";

  private String name;
  private DBManager dbManager;

  public HiveSinkDetailDao(String connectURL, String name) {
    this.dbManager = new DBManager(connectURL);
    this.name = name;
  }

  public void connect() throws SQLException {
    dbManager.connect();
  }

  public void close() throws SQLException {
    dbManager.close();
  }

  public List<String> getFinishedLogdateList(int onlineServerNum) throws SQLException {
    String sql = String.format(
        "SELECT t.logdate AS logdate FROM("
            + "SELECT logdate, COUNT(*) AS n FROM %s "
            + "WHERE state='NEW' AND name='%s' GROUP BY logdate) t "
            + "WHERE t.n >= %s",
        TABLE_NAME, name, onlineServerNum
    );
    ResultSet rs = dbManager.executeQuery(sql);
    List<String> logdateList = new ArrayList<String>();
    try {
      while (rs.next()) {
        logdateList.add(rs.getString("logdate"));
      }
    } finally {
      if (rs != null) {
        rs.close();
      }
    }
    return logdateList;
  }

  public void updateCheckedState(List<String> logdateList) throws SQLException {
    if (logdateList.size() == 0) {
      return;
    }
    String logdateSQL = "'" + Joiner.on("', '").join(logdateList) + "'";
    String sql = String.format(
        "UPDATE %s SET state='CHECKED' WHERE state='NEW' AND name='%s' AND logdate in (%s)",
        TABLE_NAME, name, logdateSQL
    );
    dbManager.execute(sql);
  }

  public boolean exists(String logdate, String hostName) throws SQLException {
    String sql = String.format(
        "SELECT * FROM %s WHERE name='%s' AND logdate='%s' AND hostname='%s'",
        TABLE_NAME, this.name, logdate, hostName
    );
    ResultSet rs = dbManager.executeQuery(sql);
    try {
      return rs.next();
    } finally {
      if (rs != null)
        rs.close();
    }
  }

  public void create(String logdate, String hostName,
                     long receiveCount, long sinkCount, long updateTimestamp) throws SQLException {
    String sql = String.format(
        "INSERT INTO %s(name, logdate, hostname, receivecount, sinkcount, updatetime) "
            + "VALUES('%s', '%s', '%s', '%s', '%s', '%s')",
        TABLE_NAME,
        this.name, logdate, hostName, receiveCount, sinkCount, updateTimestamp);
    dbManager.execute(sql);
  }

  public void update(String logdate, String hostName,
                     long receiveCount, long sinkCount, long updateTimestamp) throws SQLException {
    String sql = String.format(
        "UPDATE %s SET receivecount='%s', sinkcount='%s', updatetime='%s' "
            + "WHERE name='%s' AND logdate='%s' AND hostname='%s'",
        TABLE_NAME, receiveCount, sinkCount, updateTimestamp,
        name, logdate, hostName
    );
    dbManager.execute(sql);
  }
}
