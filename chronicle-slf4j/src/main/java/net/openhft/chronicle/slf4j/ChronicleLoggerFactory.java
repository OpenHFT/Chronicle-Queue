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

import net.openhft.chronicle.*;
import net.openhft.chronicle.slf4j.impl.ChronicleLogWriters;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.helpers.NOPLogger;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>Simple implementation of Logger that sends all enabled slf4j messages,
 * for all defined loggers, to one or more VanillaChronicle..
 * <p/>
 * To configure this sl4j binding you need to specify the location of a properties
 * files via system properties:
 * <p/>
 * <code>-Dslf4j.chronicle.properties=${pathOfYourPropertiesFile}<code>
 * </p>
 * <p/>
 * The following system properties are supported to configure the behavior of this
 * logger:
 * <p/>
 * <ul>
 * <li><code>slf4j.chronicle.path</code></li>
 * <li><code>slf4j.chronicle.level</code></li>
 * <li><code>slf4j.chronicle.shortName</code></li>
 * <li><code>slf4j.chronicle.append</code></li>
 * <li><code>slf4j.chronicle.format</code></li>
 * <li><code>slf4j.chronicle.type</code></li>
 * </ul>
 */
public class ChronicleLoggerFactory implements ILoggerFactory {
    private final Map<String, Logger> loggers;
    private final Map<String, ChronicleLogWriter> writers;
    private ChronicleLoggingConfig cfg;

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
     * <p/>
     * TODO: review error handling/messages
     */
    @Override
    public synchronized Logger getLogger(String name) {
        if (this.cfg == null) {
            System.err.println("chronicle-slf4j is not configured");
            return NOPLogger.NOP_LOGGER;
        }

        Logger logger = loggers.get(name);
        if (logger == null) {
            String path = this.cfg.getString(name, ChronicleLoggingConfig.KEY_PATH);
            String levels = this.cfg.getString(name, ChronicleLoggingConfig.KEY_LEVEL);
            int level = ChronicleLoggingHelper.stringToLevel(levels);
            Throwable error = null;

            ChronicleLogWriter writer = null;

            if (path != null) {
                writer = this.writers.get(path);
                if (writer == null) {
                    try {
                        writer = newWriter(path, name);
                    } catch (IOException e) {
                        error = e;
                        writer = null;
                    }

                    if (writer != null) {
                        this.writers.put(path, writer);
                    } else {
                        StringBuilder sb = new StringBuilder("Unable to inzialize chroncile-slf4j ")
                                .append("(")
                                .append("name")
                                .append(")");

                        if (error != null) {
                            sb.append("\n  " + error.getMessage());
                        }

                        logger = NOPLogger.NOP_LOGGER;
                    }
                }

                if (writer != null) {
                    logger = new ChronicleLogger(writer, name, level);
                    this.loggers.put(name, logger);
                }
            } else {
                StringBuilder sb = new StringBuilder("Unable to inzialize chroncile-slf4j ")
                        .append("(")
                        .append("name")
                        .append(")");

                if (path == null) {
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
        for (ChronicleLogWriter writer : this.writers.values()) {
            try {
                writer.getChronicle().close();
            } catch (IOException e) {
                System.err.println(e.getMessage());
            }
        }

        this.loggers.clear();
        this.writers.clear();
    }

    /**
     *
     */
    public synchronized void relaod() {
        shutdown();

        this.cfg = ChronicleLoggingConfig.load();
    }

    // *************************************************************************
    //
    // *************************************************************************

    /**
     * @param path
     * @param name
     * @return
     * @throws IOException
     */
    private ChronicleLogWriter newWriter(String path, String name) throws IOException {
        ChronicleLogWriter writer = null;

        String type = this.cfg.getString(name, ChronicleLoggingConfig.KEY_TYPE);
        String format = this.cfg.getString(name, ChronicleLoggingConfig.KEY_FORMAT);
        String binaryMode = this.cfg.getString(ChronicleLoggingConfig.KEY_BINARY_MODE);

        if (ChronicleLoggingConfig.FORMAT_BINARY.equalsIgnoreCase(format)) {
            Chronicle chronicle = newChronicle(type, path, name);
            if (chronicle != null) {
                writer = ChronicleLoggingConfig.BINARY_MODE_SERIALIZED.equalsIgnoreCase(binaryMode)
                        ? new ChronicleLogWriters.BinaryWriter(chronicle)
                        : new ChronicleLogWriters.BinaryFormattingWriter(chronicle);
            }
        } else if (ChronicleLoggingConfig.FORMAT_TEXT.equalsIgnoreCase(format)) {
            Chronicle chronicle = newChronicle(type, path, name);
            if (chronicle != null) {
                writer = new ChronicleLogWriters.TextWriter(
                        chronicle,
                        this.cfg.getString(name, ChronicleLoggingConfig.KEY_DATE_FORMAT)
                );
            }
        }

        if (writer != null) {
            // If the underlying chronicle is an Indexed chronicle, wrap the writer
            // so it is thread safe (synchronized)
            if (writer.getChronicle() instanceof IndexedChronicle) {
                writer = new ChronicleLogWriters.SynchronizedWriter(writer);
            }
        }

        return writer;
    }

    /**
     * @param type
     * @param path
     * @param name
     * @return
     * @throws IOException
     */
    private Chronicle newChronicle(String type, String path, String name) throws IOException {
        if (ChronicleLoggingConfig.TYPE_INDEXED.equalsIgnoreCase(type)) {
            return newIndexedChronicle(path, name);
        } else if (ChronicleLoggingConfig.TYPE_VANILLA.equalsIgnoreCase(type)) {
            return newVanillaChronicle(path, name);
        }

        return null;
    }

    /**
     * Make a VanillaChronicle with default configuration;
     *
     * @param path
     * @param name #param synchronous
     * @return
     */
    private Chronicle newVanillaChronicle(String path, String name) throws IOException {
        Boolean append = this.cfg.getBoolean(name, ChronicleLoggingConfig.KEY_APPEND, true);
        Boolean synch = this.cfg.getBoolean(name, ChronicleLoggingConfig.KEY_SYNCHRONOUS, false);

        VanillaChronicle chronicle = new VanillaChronicle(
                path,
                new VanillaChronicleConfig().synchronous(synch));

        if (append) {
            chronicle.clear();
        }

        return chronicle;
    }

    /**
     * Make an IndexedChronicle with default configuration;
     *
     * @param path
     * @param name
     * @return
     */
    private Chronicle newIndexedChronicle(String path, String name) throws IOException {
        Boolean append = this.cfg.getBoolean(name, ChronicleLoggingConfig.KEY_APPEND, true);
        Boolean synch = this.cfg.getBoolean(name, ChronicleLoggingConfig.KEY_SYNCHRONOUS, false);

        if (append) {
            new File(path + ".data").delete();
            new File(path + ".index").delete();
        }

        return new IndexedChronicle(
                path,
                ChronicleConfig.DEFAULT.clone().synchronousMode(synch));
    }
}

