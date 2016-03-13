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

CREATE TABLE `hive_sink_detail` (
	`id` INT(11) NOT NULL AUTO_INCREMENT,
	`name` CHAR(50) NOT NULL DEFAULT '0',
	`logdate` CHAR(12) NOT NULL DEFAULT '0',
	`hostname` CHAR(50) NOT NULL DEFAULT '0',
	`receivecount` BIGINT(20) NOT NULL DEFAULT '0',
	`sinkcount` BIGINT(20) NOT NULL DEFAULT '0',
	`updatetime` BIGINT(20) NOT NULL DEFAULT '0',
	`state` ENUM('NEW','CHECKED') NOT NULL DEFAULT 'NEW',
	PRIMARY KEY (`id`),
	INDEX `tablename` (`name`),
	INDEX `state` (`state`)
)
COLLATE='utf8_general_ci'
ENGINE=InnoDB
AUTO_INCREMENT=4;