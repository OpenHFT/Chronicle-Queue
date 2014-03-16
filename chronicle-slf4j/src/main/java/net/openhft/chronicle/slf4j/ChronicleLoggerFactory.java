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

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleConfig;
import net.openhft.chronicle.IndexedChronicle;
import net.openhft.chronicle.sandbox.VanillaChronicle;
import net.openhft.chronicle.sandbox.VanillaChronicleConfig;
import net.openhft.chronicle.slf4j.impl.BinaryChronicleLogWriter;
import net.openhft.chronicle.slf4j.impl.TextChronicleLogWriter;
import net.openhft.chronicle.tools.ChronicleTools;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.helpers.NOPLogger;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>Simple implementation of Logger that sends all enabled slf4j messages,
 * for all defined loggers, to one or more VanillaChronicle..
 *
 * To configure this sl4j binding you need to specify the location of a properties
 * files via system properties:
 *
 * <code>-Dslf4j.chronicle.properties=${pathOfYourPropertiesFile}<code>
 *</p>
 *
 * The following system properties are supported to configure the behavior of this
 * logger:
 *
 * <ul>
 *     <li><code>slf4j.chronicle.path</code></li>
 *     <li><code>slf4j.chronicle.level</code></li>
 *     <li><code>slf4j.chronicle.shortName</code></li>
 *     <li><code>slf4j.chronicle.append</code></li>
 *     <li><code>slf4j.chronicle.format</code></li>
 *     <li><code>slf4j.chronicle.type</code></li>
 * </ul>
 *
 */
public class ChronicleLoggerFactory implements ILoggerFactory {
    private final Map<String,Logger> loggers;
    private final Map<String,ChronicleLogWriter> writers;
    private final ChronicleLoggingConfig cfg;

    /**
     * c-tor
     */
    public ChronicleLoggerFactory() {
        this.loggers = new HashMap<String, Logger>();
        this.writers = new HashMap<String, ChronicleLogWriter>();
        this.cfg = ChronicleLoggingConfig.load();
    }

    // *************************************************************************
    //
    // *************************************************************************

    /**
     * Return an appropriate {@link ChronicleLogger} instance by name.
     *
     * TODO: review error handling/messages
     */
    @Override
    public synchronized Logger getLogger(String name) {
        if(this.cfg == null) {
            System.err.println("chronicle-slf4j is not configured");
            return NOPLogger.NOP_LOGGER;
        }

        Logger logger = loggers.get(name);
        if (logger == null) {
            String    path   = this.cfg.getString(name, ChronicleLoggingConfig.KEY_PATH);
            Boolean   append = this.cfg.getBoolean(name,ChronicleLoggingConfig.KEY_APPEND);
            String    levels = this.cfg.getString(name,ChronicleLoggingConfig.KEY_LEVEL);
            String    type   = this.cfg.getString(name,ChronicleLoggingConfig.KEY_FORMAT);
            int       level  = ChronicleLoggingHelper.stringToLevel(levels);
            Throwable error  = null;

            ChronicleLogWriter writer = null;
            if(path != null) {
                writer = this.writers.get(path);
                if(writer == null) {
                    if(ChronicleLoggingConfig.FORMAT_BINARY.equalsIgnoreCase(type)) {
                        try {
                            writer = new BinaryChronicleLogWriter(
                                newVanillaChronicle(path,append != null ? append : true)
                            );
                        } catch(IOException e) {
                            error = e;
                            writer = null;
                        }
                    } else if(ChronicleLoggingConfig.FORMAT_TEXT.equalsIgnoreCase(type)) {
                        try {
                            writer = new TextChronicleLogWriter(
                                newVanillaChronicle(path,append != null ? append : true),
                                this.cfg.getString(name,ChronicleLoggingConfig.KEY_DATE_FORMAT)
                            );
                        } catch(IOException e) {
                            error = e;
                            writer = null;
                        }
                    }

                    if(writer != null) {
                        this.writers.put(path,writer);
                    } else {
                        StringBuilder sb = new StringBuilder("Unable to inzialize chroncile-slf4j ")
                            .append("(")
                            .append("name")
                            .append(")");

                        if(error == null) {
                            sb.append("\n  slf4j.chronicle.type is not properly defined");
                            sb.append("\n    got ").append(type);
                            sb.append("\n    it must be text or binary");
                        } else {
                            sb.append("\n  " + error.getMessage());
                        }

                        writer = null;
                        logger = NOPLogger.NOP_LOGGER;
                    }
                }

                if(writer != null) {
                    logger = new ChronicleLogger(writer,name,level);
                    this.loggers.put(name, logger);
                }
            } else {
                StringBuilder sb = new StringBuilder("Unable to inzialize chroncile-slf4j ")
                    .append("(")
                    .append("name")
                    .append(")");

                if(path == null) {
                    sb.append("\n  slf4j.chronicle.path is not defined");
                    sb.append("\n  slf4j.chronicle.logger.").append(name).append(".path is not defined");
                }

                System.err.println(sb.toString());

                logger = NOPLogger.NOP_LOGGER;
            }
        }

        return logger;
    }

    /**
     *
     */
    public synchronized void warmup() {
        //TODO: preload loggers
    }

    /**
     * close underlying Chronicles
     */
    public synchronized void shutdown() {
        this.loggers.clear();
        this.writers.clear();

        for(ChronicleLogWriter writer : this.writers.values()) {
            try {
                writer.getChronicle().close();
            } catch (IOException e) {
                System.err.println(e.getMessage());
            }
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    /**
     * Make a VanillaChronicle with default configuration;
     *
     * @param path
     * @param append
     * @return
     */
    private Chronicle newVanillaChronicle(String path, boolean append) throws IOException {
        VanillaChronicle chronicle = new VanillaChronicle(path,VanillaChronicleConfig.DEFAULT);
        if(!append) {
            chronicle.clear();
        }

        return chronicle;
    }

    /**
     * Make an IndexedChronicle with default configuration;
     *
     * @param path
     * @param append
     * @return
     */
    private Chronicle newIndexedChronicle(String path, boolean append)  throws IOException {
        IndexedChronicle chronicle = new IndexedChronicle(path, ChronicleConfig.DEFAULT);
        if(!append) {
            new File(path + ".data" ).delete();
            new File(path + ".index").delete();
        }

        return chronicle;
    }
}

