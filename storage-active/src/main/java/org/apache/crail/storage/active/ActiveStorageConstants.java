/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.crail.storage.active;

import org.apache.crail.conf.CrailConfiguration;
import org.slf4j.Logger;

public class ActiveStorageConstants {
	public static final String STORAGE_ACTIVE_JAR_DIR_KEY = "crail.storage.active.jardir";
	public static String STORAGE_ACTIVE_JAR_DIR = "/tmp";

	public static final String STORAGE_ACTIVE_CORES_KEY = "crail.storage.active.cores";
	public static int STORAGE_ACTIVE_CORES = 1;

	public static final String STORAGE_ACTIVE_CHANNEL_SIZE_KEY = "crail.storage.active.chsize";
	public static int STORAGE_ACTIVE_CHANNEL_SIZE = 5;


	public static void init(CrailConfiguration conf, String[] args) {
		ActiveStorageConstants.updateConstants(conf);
	}

	public static void updateConstants(CrailConfiguration conf) {
		if (conf.get(STORAGE_ACTIVE_JAR_DIR_KEY) != null) {
			STORAGE_ACTIVE_JAR_DIR = conf.get(STORAGE_ACTIVE_JAR_DIR_KEY);
		}
		if (conf.get(STORAGE_ACTIVE_CORES_KEY) != null) {
			STORAGE_ACTIVE_CORES = Integer.parseInt(conf.get(STORAGE_ACTIVE_CORES_KEY));
		}
		if (conf.get(STORAGE_ACTIVE_CHANNEL_SIZE_KEY) != null) {
			STORAGE_ACTIVE_CHANNEL_SIZE = Integer.parseInt(conf.get(STORAGE_ACTIVE_CHANNEL_SIZE_KEY));
		}
	}

	public static void printConf(Logger logger) {
		String format = "{} {}";
		logger.info(format, STORAGE_ACTIVE_JAR_DIR_KEY, STORAGE_ACTIVE_JAR_DIR);
		logger.info(format, STORAGE_ACTIVE_CORES_KEY, STORAGE_ACTIVE_CORES);
		logger.info(format, STORAGE_ACTIVE_CHANNEL_SIZE_KEY, STORAGE_ACTIVE_CHANNEL_SIZE);
	}

}
