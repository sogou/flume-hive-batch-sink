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

package org.apache.flume.sink.hive.batch.serde;

import org.apache.flume.sink.hive.batch.util.HiveUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractDeserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by Tao Li on 2/16/16.
 */
public abstract class TextDeserializer extends AbstractDeserializer {
  private ObjectInspector rowOI;
  private List<Object> row = new ArrayList<Object>();

  public void initializeByTableName(Configuration configuration,
                                    String dbName, String tableName)
      throws TException, SerDeException {
    Properties tbl = HiveUtils.getTableColunmnProperties(dbName, tableName);
    initialize(configuration, tbl);
  }

  @Override
  public void initialize(Configuration configuration, Properties tbl) throws SerDeException {
    String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
    String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    initialize(columnNameProperty, columnTypeProperty);
  }

  public void initialize(String columnNameProperty, String columnTypeProperty) {
    List<String> columnNames = Arrays.asList(columnNameProperty.split(","));
    List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    StructTypeInfo rowTypeInfo =
        (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
    rowOI = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    row.clear();
    return deserialize(((Text) writable).getBytes(), row);
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return rowOI;
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }

  public abstract Object deserialize(byte[] bytes, List<Object> reuse);
}
