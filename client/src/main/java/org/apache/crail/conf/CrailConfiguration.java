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

package org.apache.crail.conf;

import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CrailConfiguration {
	private static final Logger LOG = CrailUtils.getLogger();
	private ConcurrentHashMap<String, String> conf;

	private CrailConfiguration() {
		conf = new ConcurrentHashMap<>();
	}

	public static CrailConfiguration createEmptyConfiguration() {
		return new CrailConfiguration();
	}

	public static CrailConfiguration createConfigurationFromFile() throws IOException {
		String base = System.getenv("CRAIL_HOME");
		if (base != null && !base.isEmpty()) {
			return createConfigurationFromFile(base + File.separator + "conf" + File.separator + "crail-site.conf");
		} else {
			throw new IllegalArgumentException("CRAIL_HOME environment variable is not set or empty");
		}
	}

	public static CrailConfiguration createConfigurationFromFile(String path) throws IOException {
		CrailConfiguration cconf = createEmptyConfiguration();
		InputStream inputStream = Files.newInputStream(Paths.get(path));
		cconf.loadConfigurationFromStream(inputStream);
		return (cconf);
	}

	public static CrailConfiguration createConfigurationFromStream(InputStream stream) throws IOException {
		CrailConfiguration cconf = createEmptyConfiguration();
		cconf.loadConfigurationFromStream(stream);
		return (cconf);
	}

	private void loadConfigurationFromStream(InputStream stream) throws IOException {
		Properties properties = loadProperties(stream);
		mergeProperties(properties);
	}

	private static String expandEnvVars(String input) throws IOException {
		if (null == input) {
			return null;
		}
		// match ${ENV_VAR_NAME} or $ENV_VAR_NAME
		Pattern p = Pattern.compile("\\$\\{(\\w+)\\}|\\$(\\w+)");
		Matcher m = p.matcher(input);
		StringBuffer output = new StringBuffer();
		while (m.find()) {
			String envVar;
			if (m.group(1) != null) {
				envVar = m.group(1);
			} else {
				envVar = m.group(2);
			}
			String envVal = System.getenv(envVar);
			if (envVal == null) {
				throw new IOException("Could not expand environment variable $" + envVar);
			}
			m.appendReplacement(output, envVal);
		}
		m.appendTail(output);
		return output.toString();
	}

	private static Properties loadProperties(InputStream inputStream) throws IOException {
		Properties properties = new Properties();

		try {
			properties.load(inputStream);
		} finally {
			inputStream.close();
		}
		for (String key : properties.stringPropertyNames()) {
			String val = properties.getProperty(key);
			properties.setProperty(key, expandEnvVars(val));
		}
		return properties;
	}

	public String get(String key) {
		return conf.get(key);
	}

	public void set(String key, String value) {
		conf.put(key, value);
	}

	public boolean getBoolean(String key, boolean fallback) {
		if (conf.containsKey(key)) {
			return Boolean.parseBoolean(conf.get(key));
		} else {
			return fallback;
		}
	}

	public void setInt(String key, int level) {
		String value = Integer.toString(level);
		conf.put(key, value);
	}

	private void mergeProperties(Properties properties) {
		if (properties == null) {
			return;
		}
		for (String key : properties.stringPropertyNames()) {
			conf.put(key.trim(), properties.getProperty(key).trim());
		}
	}
}
