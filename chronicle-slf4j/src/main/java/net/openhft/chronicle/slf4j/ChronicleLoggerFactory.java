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
import net.openhft.chronicle.slf4j.impl.SynchronizedChronicleLogWriter;
import net.openhft.chronicle.slf4j.impl.TextChronicleLogWriter;
import net.openhft.chronicle.tools.ChronicleTools;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.helpers.NOPLogger;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
        this(ChronicleLoggingConfig.load());
    }

    /**
     * c-tor
     */
    public ChronicleLoggerFactory(final ChronicleLoggingConfig cfg) {
        this.loggers = new ConcurrentHashMap<String, Logger>();
        this.writers = new ConcurrentHashMap<String, ChronicleLogWriter>();
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
            String    type   = this.cfg.getString(ChronicleLoggingConfig.KEY_TYPE);
            String    path   = this.cfg.getString(name, ChronicleLoggingConfig.KEY_PATH);
            Boolean   append = this.cfg.getBoolean(name,ChronicleLoggingConfig.KEY_APPEND);
            String    levels = this.cfg.getString(name,ChronicleLoggingConfig.KEY_LEVEL);
            String    format = this.cfg.getString(name,ChronicleLoggingConfig.KEY_FORMAT);
            int       level  = ChronicleLoggingHelper.stringToLevel(levels);
            Throwable error  = null;

            ChronicleLogWriter writer = null;

            if(path != null && type != null) {
                writer = this.writers.get(path);
                if(writer == null) {
                    try {
                        writer = newWriter(name,type,format,path,append != null ? append : true);
                    } catch(IOException e) {
                        error = e;
                        writer = null;
                    }

                    if(writer != null) {
                        this.writers.put(path,writer);
                    } else {
                        StringBuilder sb = new StringBuilder("Unable to inzialize chroncile-slf4j ")
                            .append("(")
                            .append("name")
                            .append(")");

                        if(error == null) {
                            sb.append("\n  slf4j.chronicle.type may not properly defined");
                            sb.append("\n    got ").append(type);
                            sb.append("\n    it must be indexed or vanilla");
                            sb.append("\n  slf4j.chronicle.format may not properly defined");
                            sb.append("\n    got ").append(format);
                            sb.append("\n    it must be text or binary");
                        } else {
                            sb.append("\n  " + error.getMessage());
                        }

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

                if(type == null) {
                    sb.append("\n  slf4j.chronicle.type is not defined");
                }

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

    // *************************************************************************
    //
    // *************************************************************************

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
     *
     * @param name
     * @param type
     * @param format
     * @param path
     * @param append
     * @return
     * @throws IOException
     */
    private ChronicleLogWriter newWriter(String name, String type, String format, String path, boolean append) throws IOException {
        ChronicleLogWriter writer = null;

        if(ChronicleLoggingConfig.FORMAT_BINARY.equalsIgnoreCase(format)) {
            Chronicle chronicle = newChronicle(type,path,append);
            if(chronicle != null) {
                writer = new BinaryChronicleLogWriter(chronicle);
            }
        } else if(ChronicleLoggingConfig.FORMAT_TEXT.equalsIgnoreCase(format)) {
            Chronicle chronicle = newChronicle(type,path,append);
            if(chronicle != null) {
                writer = new TextChronicleLogWriter(
                    chronicle,
                    this.cfg.getString(name, ChronicleLoggingConfig.KEY_DATE_FORMAT)
                );
            }
        }

        if(writer != null) {
            // If the underlying chronicle is an Indexed chronicle, wrap the writer
            // so it is thread safe (synchronized)
            if(writer.getChronicle() instanceof IndexedChronicle) {
                writer = new SynchronizedChronicleLogWriter(writer);
            }
        }

        return writer;
    }

    /**
     *
     * @param type
     * @param path
     * @param append
     * @return
     * @throws IOException
     */
    private Chronicle newChronicle(String type, String path, boolean append) throws IOException {
        if(ChronicleLoggingConfig.TYPE_INDEXED.equalsIgnoreCase(type)) {
            return newIndexedChronicle(path,append);
        } else if(ChronicleLoggingConfig.TYPE_VANILLA.equalsIgnoreCase(type)) {
            return newVanillaChronicle(path, append);
        }

        return null;
    }

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

