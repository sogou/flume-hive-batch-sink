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

package org.apache.flume.sink.hive.batch.util;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by Tao Li on 2016/3/1.
 */
public class CommonUtils {
  public static String getStackTraceStr(Exception exception) {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    exception.printStackTrace(new PrintStream(stream));
    return stream.toString();
  }

  public static long convertTimeStringToTimestamp(String timeString, String timeFormat)
      throws ParseException {
    SimpleDateFormat sdf = new SimpleDateFormat(timeFormat);
    return sdf.parse(timeString).getTime();
  }

  public static long getMillisecond(long num, int unit) {
    if (unit == Calendar.SECOND) {
      return num * 1000;
    } else if (unit == Calendar.MINUTE) {
      return num * 60 * 1000;
    } else if (unit == Calendar.HOUR_OF_DAY) {
      return num * 3600 * 1000;
    }
    throw new IllegalArgumentException("unknown time unit: " + unit);
  }
}
