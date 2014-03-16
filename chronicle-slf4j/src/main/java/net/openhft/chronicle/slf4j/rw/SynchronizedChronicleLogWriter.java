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
package net.openhft.chronicle.slf4j.rw;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.slf4j.ChronicleLogWriter;

import java.io.Closeable;
import java.io.IOException;

/**
 *
 */
public class SynchronizedChronicleLogWriter implements ChronicleLogWriter, Closeable {
    private final ChronicleLogWriter writer;
    private final Object sync;

    /**
     *
     * @param writer
     */
    public SynchronizedChronicleLogWriter(final ChronicleLogWriter writer) {
        this.writer = writer;
        this.sync = new Object();
    }

    @Override
    public Chronicle getChronicle() {
        return this.writer.getChronicle();
    }

    @Override
    public void log(int level, String name, String message, Throwable t) {
        synchronized (this.sync) {
            this.writer.log(level,name,message,t);
        }

    }

    @Override
    public void close() throws IOException {
        this.writer.close();
    }
}
