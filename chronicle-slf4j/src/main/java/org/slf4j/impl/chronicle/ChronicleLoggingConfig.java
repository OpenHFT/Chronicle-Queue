package org.slf4j.impl.chronicle;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * @author lburgazzoli
 */
public class ChronicleLoggingConfig {
    public static final String KEY_PROPERTIES_FILE      = "slf4j.chronicle.properties";
    public static final String KEY_PREFIX               = "slf4j.chronicle.";
    public static final String KEY_LOGER                = "logger";
    public static final String KEY_LEVEL                = "level";
    public static final String KEY_PATH                 = "path";
    public static final String KEY_SHORTNAME            = "shortName";
    public static final String KEY_APPEND               = "append";
    public static final String KEY_TYPE                 = "type";
    public static final String PLACEHOLDER_START        = "${";
    public static final String PLACEHOLDER_END          = "}";
    public static final String PLACEHOLDER_TODAY        = "${today}";
    public static final String PLACEHOLDER_TODAY_FORMAT = "yyyyMMdd";
    public static final String PLACEHOLDER_PID          = "${pid}";

    private static final DateFormat DATEFORMAT = new SimpleDateFormat(PLACEHOLDER_TODAY_FORMAT);
    private static final String     PID        = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];

    private final Properties properties;

    /**
     * c-tor
     */
    private ChronicleLoggingConfig(final Properties properties) {
        this.properties = properties;
    }

    /**
     * TODO: review
     *
     * @return
     */
    public static ChronicleLoggingConfig load() {
        Properties tmpProperties = new Properties();
        Properties locProperties = new Properties();
        String cfgPath = System.getProperty(KEY_PROPERTIES_FILE);
        InputStream in = null;

        if(cfgPath == null) {
            in = ChronicleLoggingConfig.class.getClassLoader().getResourceAsStream("META-INF/" + KEY_PROPERTIES_FILE);
        } else {
            try {
                in = new FileInputStream(cfgPath);
            } catch(Exception e) {
                in = null;
                // ignored
            }
        }

        if (null != in) {
            try {
                tmpProperties.load(in);
                in.close();
            } catch (IOException e) {
                // ignored
            }
        }

        //TODO: re-engine
        for(Object key : tmpProperties.keySet()) {
            String val = tmpProperties.getProperty((String)key);
            val = val.replace(PLACEHOLDER_TODAY, DATEFORMAT.format(new Date()));
            val = val.replace(PLACEHOLDER_PID, PID);

            int startIndex = 0;
            int endIndex = 0;

            do {
                startIndex = val.indexOf(PLACEHOLDER_START,endIndex);
                if(startIndex != -1) {
                    endIndex = val.indexOf(PLACEHOLDER_END,startIndex);
                    if(endIndex != -1) {
                        String envKey = val.substring(startIndex+2,endIndex);
                        String newVal = null;
                        if(tmpProperties.containsKey(envKey)) {
                            newVal = tmpProperties.getProperty(envKey);
                        } else if(System.getProperties().containsKey(envKey)){
                            newVal = System.getProperties().getProperty(envKey);
                        }

                        if(newVal != null) {
                            val = val.replace(PLACEHOLDER_START + envKey + PLACEHOLDER_END,newVal);
                            endIndex += newVal.length() - envKey.length() + 3;
                        }
                    }
                }
            } while(startIndex != -1 && endIndex != -1 && endIndex < val.length());

            locProperties.put(key,val);
        }

        return new ChronicleLoggingConfig(locProperties);
    }

    /**
     *
     * @param shortName
     * @return
     */
    public String getString(final String shortName) {
        String name = KEY_PREFIX + shortName;
        return this.properties.getProperty(name);
    }

    /**
     *
     * @param shortName
     * @return
     */
    public String getString(final String loggerName, final String shortName) {
        String key = KEY_PREFIX + KEY_LOGER + "." + loggerName + "." + shortName;
        String val = this.properties.getProperty(key);

        if(val == null) {
            val = getString(shortName);
        }

        return val;
    }

    /**
     *
     * @param shortName
     * @return
     */
    public Boolean getBoolean(final String shortName) {
        String prop = getString(shortName);
        return (prop != null) ? "true".equalsIgnoreCase(prop) : null;
    }

    /**
     *
     * @param shortName
     * @return
     */
    public Boolean getBoolean(final String loggerName, final String shortName) {
        String prop = getString(loggerName,shortName);
        return (prop != null) ? "true".equalsIgnoreCase(prop) : null;
    }

    /**
     *
     * @param shortName
     * @return
     */
    public Integer getInteger(final String shortName) {
        String prop = getString(shortName);
        return (prop != null) ? Integer.parseInt(prop) : null;
    }

    /**
     *
     * @param shortName
     * @return
     */
    public Integer getInteger(final String loggerName, final String shortName) {
        String prop = getString(loggerName,shortName);
        return (prop != null) ? Integer.parseInt(prop) : null;
    }

    /**
     *
     * @param shortName
     * @return
     */
    public Long getLong(final String shortName) {
        String prop = getString(shortName);
        return (prop != null) ? Long.parseLong(prop) : null;
    }

    /**
     *
     * @param shortName
     * @return
     */
    public Long getLong(final String loggerName, final String shortName) {
        String prop = getString(loggerName,shortName);
        return (prop != null) ? Long.parseLong(prop) : null;
    }

    /**
     *
     * @param shortName
     * @return
     */
    public Double getDouble(final String shortName) {
        String prop = getString(shortName);
        return (prop != null) ? Double.parseDouble(prop) : null;
    }

    /**
     *
     * @param shortName
     * @return
     */
    public Double getDouble(final String loggerName, final String shortName) {
        String prop = getString(loggerName,shortName);
        return (prop != null) ? Double.parseDouble(prop) : null;
    }

    /**
     *
     * @param shortName
     * @return
     */
    public Short getShort(final String shortName) {
        String prop = getString(shortName);
        return (prop != null) ? Short.parseShort(prop) : null;
    }

    /**
     *
     * @param shortName
     * @return
     */
    public Short getShort(final String loggerName, final String shortName) {
        String prop = getString(loggerName,shortName);
        return (prop != null) ? Short.parseShort(prop) : null;
    }
}
