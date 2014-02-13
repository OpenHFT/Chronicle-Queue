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
package net.openhft.chronicle.sandbox.slf4j;

import net.openhft.chronicle.sandbox.VanillaChronicleConfig;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class ChronicleLoggerFactory implements ILoggerFactory {
    private final ConcurrentMap<String,Logger> loggers;
    private final Properties properties;
    private final ChronicleWriter writer;

    /**
     * c-tor
     */
    public ChronicleLoggerFactory() {
        this.loggers = new ConcurrentHashMap<String, Logger>();
        this.properties = ChronicleHelper.loadProperties();

        String  path   = ChronicleHelper.getStringProperty(this.properties,ChronicleHelper.KEY_PATH);
        Boolean append = ChronicleHelper.getBooleanProperty(this.properties,ChronicleHelper.KEY_APPEND);

        if(path != null) {
            this.writer = new ChronicleWriter(path,append != null ? append : false, VanillaChronicleConfig.DEFAULT);
        } else {
            this.writer = null;
        }
    }

    /**
     * Return an appropriate {@link ChronicleLogger} instance by name.
     */
    public Logger getLogger(String name) {
        Logger logger = loggers.get(name);
        if (logger == null) {
            String  level  = ChronicleHelper.getStringProperty(this.properties, ChronicleHelper.KEY_LEVEL);
            if(this.writer != null && level != null) {
                Logger newInstance = new ChronicleLogger(this.writer,name,ChronicleHelper.stringToLevel(level));
                Logger oldInstance = loggers.putIfAbsent(name, newInstance);
                logger = oldInstance == null ? newInstance : oldInstance;
            }
        }

        return logger;
    }
}

