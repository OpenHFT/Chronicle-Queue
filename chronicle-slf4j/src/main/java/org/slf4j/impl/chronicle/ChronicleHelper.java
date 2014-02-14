/*
 * Copyright 2014 Peter Lawrey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.slf4j.impl.chronicle;

import org.slf4j.spi.LocationAwareLogger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * TODO: tmp class
 */
public class ChronicleHelper {
    public static final int LOG_LEVEL_TRACE = LocationAwareLogger.TRACE_INT;
    public static final int LOG_LEVEL_DEBUG = LocationAwareLogger.DEBUG_INT;
    public static final int LOG_LEVEL_INFO  = LocationAwareLogger.INFO_INT;
    public static final int LOG_LEVEL_WARN  = LocationAwareLogger.WARN_INT;
    public static final int LOG_LEVEL_ERROR = LocationAwareLogger.ERROR_INT;

    public static final String LOG_LEVEL_TRACE_S = "trace";
    public static final String LOG_LEVEL_DEBUG_S = "debug";
    public static final String LOG_LEVEL_INFO_S  = "info";
    public static final String LOG_LEVEL_WARN_S  = "warn";
    public static final String LOG_LEVEL_ERROR_S = "error";

    public static final int    DEFAULT_LOG_LEVEL   = LOG_LEVEL_INFO;
    public static final String DEFAULT_LOG_LEVEL_S = LOG_LEVEL_INFO_S;

    public static final String TRUE_S  = "true";
    public static final String FALSE_S = "false";
    public static final String TYPE_BINARY = "binary";
    public static final String TYPE_TEXT   = "text";

    public static final String KEY_PROPERTIES_FILE = "org.slf4j.logger.chronicle.properties";
    public static final String CFG_PREFIX          = "org.slf4j.logger.chronicle.";
    public static final String KEY_LEVEL           = "level";
    public static final String KEY_PATH            = "path";
    public static final String KEY_SHORTNAME       = "shortName";
    public static final String KEY_APPEND          = "append";
    public static final String KEY_TYPE            = "type";

    /**
     *
     * @param name
     * @return
     */
    public static String computeShortName(final String name) {
        return name.substring(name.lastIndexOf(".") + 1);
    }

    /**
     *
     * @param levelStr
     * @return
     */
    public static int stringToLevel(String levelStr) {
        if(levelStr != null) {
            if (LOG_LEVEL_TRACE_S.equalsIgnoreCase(levelStr)) {
                return LOG_LEVEL_TRACE;
            } else if (LOG_LEVEL_DEBUG_S.equalsIgnoreCase(levelStr)) {
                return LOG_LEVEL_DEBUG;
            } else if (LOG_LEVEL_INFO_S.equalsIgnoreCase(levelStr)) {
                return LOG_LEVEL_INFO;
            } else if (LOG_LEVEL_WARN_S.equalsIgnoreCase(levelStr)) {
                return LOG_LEVEL_WARN;
            } else if (LOG_LEVEL_ERROR_S.equalsIgnoreCase(levelStr)) {
                return LOG_LEVEL_ERROR;
            }
        }

        return DEFAULT_LOG_LEVEL;
    }

    /**
     * TODO: review
     *
     * @return
     */
    public static Properties loadProperties() {
        Properties properties = new Properties();
        String propertyLocation = System.getProperty(KEY_PROPERTIES_FILE);
        InputStream in = null;

        if(propertyLocation == null) {
            in = ChronicleHelper.class.getClassLoader().getResourceAsStream("META-INF/" + KEY_PROPERTIES_FILE);
        } else {
            try {
                in = new FileInputStream(propertyLocation);
            } catch(Exception e) {
                in = null;
                // ignored
            }
        }

        if (null != in) {
            try {
                properties.load(in);
                in.close();
            } catch (IOException e) {
                // ignored
            }
        }

        for(Object key : properties.keySet()) {
            String val = properties.getProperty((String)key);
            if(val.startsWith("${env.") && val.endsWith("}")) {
                String envKey = val.substring(6, val.length() - 1);
                String envVal = System.getenv(envKey);
                if(envVal != null) {
                    properties.put(key,envVal);
                }
            } else if(val.startsWith("${") && val.endsWith("}")) {
                String systemKey = val.substring(2, val.length() - 1);
                String systemVal = System.getProperty(systemKey);
                if(systemKey != null) {
                    properties.put(key,systemVal);
                }
            }
        }

        return properties;
    }



    /**
     *
     * @param properties
     * @param shortName
     * @return
     */
    public static String getStringProperty(final Properties properties,final String shortName) {
        String name = CFG_PREFIX + shortName;
        String prop = System.getProperty(name);
        return (prop != null) ? prop : properties.getProperty(name);
    }

    /**
     *
     * @param properties
     * @param shortName
     * @return
     */
    public static Boolean getBooleanProperty(final Properties properties,final String shortName) {
        String prop = getStringProperty(properties,shortName);
        return (prop != null) ? "true".equalsIgnoreCase(prop) : null;
    }

    /**
     *
     * @param properties
     * @param shortName
     * @return
     */
    public static Integer getIntegerProperty(final Properties properties,final String shortName) {
        String prop = getStringProperty(properties,shortName);
        return (prop != null) ? Integer.parseInt(prop) : null;
    }

    /**
     *
     * @param properties
     * @param shortName
     * @return
     */
    public static Long getLongProperty(final Properties properties,final String shortName) {
        String prop = getStringProperty(properties,shortName);
        return (prop != null) ? Long.parseLong(prop) : null;
    }

    /**
     *
     * @param properties
     * @param shortName
     * @return
     */
    public static Double getDoubleProperty(final Properties properties,final String shortName) {
        String prop = getStringProperty(properties,shortName);
        return (prop != null) ? Double.parseDouble(prop) : null;
    }

    /**
     *
     * @param properties
     * @param shortName
     * @return
     */
    public static Short getShortProperty(final Properties properties,final String shortName) {
        String prop = getStringProperty(properties,shortName);
        return (prop != null) ? Short.parseShort(prop) : null;
    }
}
