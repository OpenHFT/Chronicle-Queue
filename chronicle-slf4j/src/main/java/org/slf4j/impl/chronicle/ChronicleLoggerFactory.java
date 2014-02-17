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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class ChronicleLoggerFactory implements ILoggerFactory {
    private final ConcurrentMap<String,Logger> loggers;
    private final ConcurrentMap<String,ChronicleWriter> writers;
    private final ChronicleLoggingConfig cfg;

    /**
     * c-tor
     */
    public ChronicleLoggerFactory() {
        this.loggers = new ConcurrentHashMap<String, Logger>();
        this.writers = new ConcurrentHashMap<String, ChronicleWriter>();
        this.cfg = ChronicleLoggingConfig.load();
    }

    /**
     * Return an appropriate {@link ChronicleLogger} instance by name.
     */
    @Override
    public synchronized Logger getLogger(String name) {
        Logger logger = loggers.get(name);
        if (logger == null) {
            String  path   = this.cfg.getString(name, ChronicleLoggingConfig.KEY_PATH);
            Boolean append = this.cfg.getBoolean(name,ChronicleLoggingConfig.KEY_APPEND);
            String  levels = this.cfg.getString(name,ChronicleLoggingConfig.KEY_LEVEL);
            int     level  = ChronicleLoggingHelper.stringToLevel(levels);

            ChronicleWriter writer = null;
            if(path != null) {
                writer = this.writers.get(path);
                if(writer == null) {
                    writer = new BinaryChronicleWriter(
                        path,
                        append != null ? append : true,
                        VanillaChronicleConfig.DEFAULT);

                    this.writers.put(path,writer);
                }

                if(writer != null) {
                    Logger newInstance = new ChronicleLogger(writer,name,level);
                    Logger oldInstance = loggers.putIfAbsent(name, newInstance);
                    logger = oldInstance == null ? newInstance : oldInstance;
                }

            } else {
                StringBuilder sb = new StringBuilder("Unable to inzialize slf4j-chronicle ")
                    .append("(")
                    .append("name")
                    .append(")");

                if(path == null) {
                    sb.append("\n  slf4j.chronicle.path is not defined");
                }

                System.out.println(sb.toString());
            }
        }

        return logger;
    }
}

