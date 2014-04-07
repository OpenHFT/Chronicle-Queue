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
package net.openhft.chronicle.slf4j;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

/**
 * @author lburgazzoli
 *         <p/>
 *         Configurationn example:
 *         # default
 *         slf4j.chronicle.base = ${java.io.tmpdir}/chronicle/${today}/${pid}
 *         <p/>
 *         # logger : root
 *         slf4j.chronicle.type      = vanilla
 *         slf4j.chronicle.path      = ${slf4j.chronicle.base}/root
 *         slf4j.chronicle.level     = debug
 *         slf4j.chronicle.shortName = false
 *         slf4j.chronicle.append    = false
 *         slf4j.chronicle.format    = binary
 *         slf4j.chronicle.serialize = false
 *         <p/>
 *         # logger : Logger1
 *         slf4j.chronicle.logger.Logger1.path = ${slf4j.chronicle.base}/logger_1
 *         slf4j.chronicle.logger.Logger1.level = info
 */
public class ChronicleLoggingConfig {
    public static final String KEY_PROPERTIES_FILE = "slf4j.chronicle.properties";
    public static final String KEY_PREFIX = "slf4j.chronicle.";
    public static final String KEY_LOGER = "logger";
    public static final String KEY_LEVEL = "level";
    public static final String KEY_PATH = "path";
    public static final String KEY_SHORTNAME = "shortName";
    public static final String KEY_APPEND = "append";
    public static final String KEY_SYNCHRONOUS = "synchronous";
    public static final String KEY_FORMAT = "format";
    public static final String KEY_TYPE = "type";
    public static final String KEY_DATE_FORMAT = "dateFormat";
    public static final String KEY_BINARY_MODE = "binaryMode";
    public static final String FORMAT_BINARY = "binary";
    public static final String FORMAT_TEXT = "text";
    public static final String TYPE_VANILLA = "vanilla";
    public static final String TYPE_INDEXED = "indexed";
    public static final String BINARY_MODE_FORMATTED = "formatted";
    public static final String BINARY_MODE_SERIALIZED = "serialized";
    public static final String PLACEHOLDER_START = "${";
    public static final String PLACEHOLDER_END = "}";
    public static final String PLACEHOLDER_TODAY = "${today}";
    public static final String PLACEHOLDER_TODAY_FORMAT = "yyyyMMdd";
    private static final DateFormat DATEFORMAT = new SimpleDateFormat(PLACEHOLDER_TODAY_FORMAT);
    public static final String PLACEHOLDER_PID = "${pid}";
    public static final String DEFAULT_DATE_FORMAT = "yyyy.MM.dd-HH:mm:ss.SSS";
    private static final String PID = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];

    private final Properties properties;

    /**
     * c-tor
     */
    private ChronicleLoggingConfig(final Properties properties) {
        this.properties = properties;
    }

    // *************************************************************************
    //
    // *************************************************************************

    /**
     * @param cfgPath
     * @return
     */
    public static ChronicleLoggingConfig load(String cfgPath) {
        Properties properties = new Properties();

        try {
            InputStream in = new FileInputStream(cfgPath);

            try {
                properties.load(in);
                in.close();
            } catch (IOException e) {
                // ignored
            }

            interpolate(properties);

        } catch (Exception e) {
            e.printStackTrace(System.err);
        }

        return new ChronicleLoggingConfig(properties);
    }

    /**
     * @return
     */
    public static ChronicleLoggingConfig load() {
        String cfgPath = System.getProperty(KEY_PROPERTIES_FILE);
        if (cfgPath == null) {
            System.err.printf(
                    "Unable to configure chroncile-slf4j, property %s is not defined\n",
                    KEY_PROPERTIES_FILE);

            return null;
        }

        return load(cfgPath);
    }

    // *************************************************************************
    //
    // *************************************************************************

    /**
     * @param tmpProperties
     */
    private static void interpolate(final Properties tmpProperties) {
        int amended = 0;
        do {
            amended = 0;
            //TODO: re-engine
            for (Map.Entry<Object, Object> entries : tmpProperties.entrySet()) {
                String val = tmpProperties.getProperty((String) entries.getKey());
                val = val.replace(PLACEHOLDER_TODAY, DATEFORMAT.format(new Date()));
                val = val.replace(PLACEHOLDER_PID, PID);

                int startIndex = 0;
                int endIndex = 0;

                do {
                    startIndex = val.indexOf(PLACEHOLDER_START, endIndex);
                    if (startIndex != -1) {
                        endIndex = val.indexOf(PLACEHOLDER_END, startIndex);
                        if (endIndex != -1) {
                            String envKey = val.substring(startIndex + 2, endIndex);
                            String newVal = null;
                            if (tmpProperties.containsKey(envKey)) {
                                newVal = tmpProperties.getProperty(envKey);
                            } else if (System.getProperties().containsKey(envKey)) {
                                newVal = System.getProperties().getProperty(envKey);
                            }

                            if (newVal != null) {
                                val = val.replace(PLACEHOLDER_START + envKey + PLACEHOLDER_END, newVal);
                                endIndex += newVal.length() - envKey.length() + 3;

                                amended++;
                            }
                        }
                    }
                } while (startIndex != -1 && endIndex != -1 && endIndex < val.length());

                entries.setValue(val);
            }
        } while (amended > 0);
    }

    /**
     * @param shortName
     * @return
     */
    public String getString(final String shortName) {
        String name = KEY_PREFIX + shortName;
        return this.properties.getProperty(name);
    }

    /**
     * @param shortName
     * @return
     */
    public String getString(final String loggerName, final String shortName) {
        String key = KEY_PREFIX + KEY_LOGER + "." + loggerName + "." + shortName;
        String val = this.properties.getProperty(key);

        if (val == null) {
            val = getString(shortName);
        }

        return val;
    }

    /**
     * @param shortName
     * @return
     */
    public Boolean getBoolean(final String shortName) {
        String prop = getString(shortName);
        return (prop != null) ? "true".equalsIgnoreCase(prop) : null;
    }

    /**
     * @param shortName
     * @return
     */
    public Boolean getBoolean(final String shortName, boolean defval) {
        String prop = getString(shortName);
        return (prop != null) ? "true".equalsIgnoreCase(prop) : defval;
    }

    /**
     * @param shortName
     * @return
     */
    public Boolean getBoolean(final String loggerName, final String shortName) {
        String prop = getString(loggerName, shortName);
        return (prop != null) ? "true".equalsIgnoreCase(prop) : null;
    }

    /**
     * @param loggerName
     * @param shortName
     * @param defval
     * @return
     */
    public Boolean getBoolean(final String loggerName, final String shortName, boolean defval) {
        String prop = getString(loggerName, shortName);
        return (prop != null) ? "true".equalsIgnoreCase(prop) : defval;
    }

    /**
     * @param shortName
     * @return
     */
    public Integer getInteger(final String shortName) {
        String prop = getString(shortName);
        return (prop != null) ? Integer.parseInt(prop) : null;
    }

    /**
     * @param shortName
     * @return
     */
    public Integer getInteger(final String loggerName, final String shortName) {
        String prop = getString(loggerName, shortName);
        return (prop != null) ? Integer.parseInt(prop) : null;
    }

    /**
     * @param shortName
     * @return
     */
    public Long getLong(final String shortName) {
        String prop = getString(shortName);
        return (prop != null) ? Long.parseLong(prop) : null;
    }

    /**
     * @param shortName
     * @return
     */
    public Long getLong(final String loggerName, final String shortName) {
        String prop = getString(loggerName, shortName);
        return (prop != null) ? Long.parseLong(prop) : null;
    }

    /**
     * @param shortName
     * @return
     */
    public Double getDouble(final String shortName) {
        String prop = getString(shortName);
        return (prop != null) ? Double.parseDouble(prop) : null;
    }

    /**
     * @param shortName
     * @return
     */
    public Double getDouble(final String loggerName, final String shortName) {
        String prop = getString(loggerName, shortName);
        return (prop != null) ? Double.parseDouble(prop) : null;
    }

    /**
     * @param shortName
     * @return
     */
    public Short getShort(final String shortName) {
        String prop = getString(shortName);
        return (prop != null) ? Short.parseShort(prop) : null;
    }

    // *************************************************************************
    //
    // *************************************************************************

    /**
     * @param shortName
     * @return
     */
    public Short getShort(final String loggerName, final String shortName) {
        String prop = getString(loggerName, shortName);
        return (prop != null) ? Short.parseShort(prop) : null;
    }
}
