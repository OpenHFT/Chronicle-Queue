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
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.sandbox.VanillaChronicle;
import net.openhft.chronicle.sandbox.VanillaChronicleConfig;

import java.io.Closeable;
import java.io.IOException;

/**
 *
 */
public class BinaryChronicleLogWriter implements ChronicleLogWriter, Closeable {

    private final String path;
    private final boolean append;
    private final VanillaChronicle chronicle;
    private final ExcerptAppender appender;

    /**
     *
     * @param path
     * @param append
     * @param config
     */
    public BinaryChronicleLogWriter(String path, boolean append, VanillaChronicleConfig config) throws IOException {
        this.path = path;
        this.append = append;
        this.chronicle = new VanillaChronicle(path,config);
        this.appender = this.chronicle.createAppender();

        if(!append) {
            this.chronicle.clear();
        }
    }

    @Override
    public Chronicle getChronicle() {
        return this.chronicle;
    }

    /**
     * This is the internal implementation for logging regular (non-parameterized)
     * slf4j messages.
     *
     * long   : timestamp
     * int    : level
     * String : name
     * String : message
     * String : t.getMessage() ????
     *
     * @param level   One of the LOG_LEVEL_XXX constants defining the slf4j level
     * @param name    The logger name
     * @param message The message itself
     * @param t       The exception whose stack trace should be logged
     */
    @Override
    public void log(int level, String name, String message, Throwable t) {
        final Thread currentThread = Thread.currentThread();

        this.appender.startExcerpt();
        this.appender.writeLong(System.currentTimeMillis());
        this.appender.writeByte(level);
        this.appender.writeLong(currentThread.getId());
        this.appender.writeEnum(currentThread.getName());
        this.appender.writeEnum(name);
        this.appender.writeEnum(message);
        //TODO: write Throwable
        //appender.writeEnum(t.getMessage());
        this.appender.finish();
    }

    @Override
    public void close() throws IOException {
        if(this.chronicle != null) {
            this.chronicle.close();
        }
    }
}
