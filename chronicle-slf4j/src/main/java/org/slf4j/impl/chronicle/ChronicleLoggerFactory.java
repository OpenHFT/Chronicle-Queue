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
    private final BinaryChronicleWriter writer;
    private final int level;

    /**
     * c-tor
     */
    public ChronicleLoggerFactory() {
        this.loggers = new ConcurrentHashMap<String, Logger>();
        this.properties = ChronicleHelper.loadProperties();

        String  path   = ChronicleHelper.getStringProperty(this.properties,ChronicleHelper.KEY_PATH);
        Boolean append = ChronicleHelper.getBooleanProperty(this.properties,ChronicleHelper.KEY_APPEND);
        String  type   = ChronicleHelper.getStringProperty(this.properties, ChronicleHelper.KEY_TYPE);
        String  levels = ChronicleHelper.getStringProperty(this.properties, ChronicleHelper.KEY_LEVEL);

        this.level = ChronicleHelper.stringToLevel(levels);

        //TODO: add support for text logging?
        if(path != null && ChronicleHelper.TYPE_BINARY.equalsIgnoreCase(type)) {
            this.writer = new BinaryChronicleWriter(path,append != null ? append : false, VanillaChronicleConfig.DEFAULT);
        } else {
            this.writer = null;

            StringBuilder sb = new StringBuilder("Unable to inzialize slf4j-chronicle");
            if(path == null) {
                sb.append("\n  org.slf4j.logger.chronicle.path is not defined");
            }
            if(!ChronicleHelper.TYPE_BINARY.equalsIgnoreCase(type)) {
                sb.append("\n  org.slf4j.logger.chronicle.type is not properly defined");
            }

            System.out.println(sb.toString());
        }
    }

    /**
     * Return an appropriate {@link ChronicleLogger} instance by name.
     */
    public Logger getLogger(String name) {
        Logger logger = loggers.get(name);
        if (logger == null) {
            if(this.writer != null) {
                Logger newInstance = new ChronicleLogger(this.writer,name,this.level);
                Logger oldInstance = loggers.putIfAbsent(name, newInstance);
                logger = oldInstance == null ? newInstance : oldInstance;
            }
        }

        return logger;
    }
}

