# Flume Hive Batch Sink

[![Build Status](https://travis-ci.org/litao-buptsse/flume-hive-batch-sink.svg?branch=master)](https://travis-ci.org/litao-buptsse/flume-hive-batch-sink)
[![Software License](https://img.shields.io/badge/license-Apache%202.0-brightgreen.svg)](https://github.com/litao-buptsse/flume-hive-batch-sink/blob/master/LICENSE)

---

Flume Hive Batch Sink is a flume plugin to sink event to hive in a batch mode.



## Getting Started

---

Compile the project:

```
mvn package
```

Example Configuration:

```
a1.sinks.k1.type = org.apache.flume.sink.hive.batch.HiveBatchSink
a1.sinks.k1.hive.database = default
a1.sinks.k1.hive.table = mytable
a1.sinks.k1.hive.path=/user/hive/warehouse/default.db/mytable/%Y%m/%Y%m%d
a1.sinks.k1.hive.partition = logdate=%Y%m%d%H%M
a1.sinks.k1.hive.filePrefix = mytable-%Y%m%d%H%M
a1.sinks.k1.hive.serde = com.example.serde.MySerde
a1.sinks.k1.hive.round = true
a1.sinks.k1.hive.roundValue = 5
a1.sinks.k1.hive.roundUnit = minute
```

## Features

---

* Sink flume event to hive table in a batch mode.
* Support add hive parition.
* With orc storage.
* Support custom hive serde.
* Better sink counter and support persist the counter data to database.
* Leader election when distributed deployed. The leader can determine if all the sink have finished their batch work.