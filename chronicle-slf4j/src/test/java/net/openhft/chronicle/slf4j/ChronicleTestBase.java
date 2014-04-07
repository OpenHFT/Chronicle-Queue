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

import net.openhft.chronicle.IndexedChronicle;
import net.openhft.chronicle.VanillaChronicle;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import net.openhft.lang.model.constraints.NotNull;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.StaticLoggerBinder;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 */
public class ChronicleTestBase {

    // *************************************************************************
    //
    // *************************************************************************

    protected static String basePath(String type) {
        return System.getProperty("java.io.tmpdir")
                + File.separator
                + "chronicle"
                + File.separator
                + type
                + File.separator
                + new SimpleDateFormat("yyyyMMdd").format(new Date())
                + File.separator
                + ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
    }

    protected static String basePath(String type, String loggerName) {
        return basePath(type)
                + File.separator
                + loggerName;
    }

    // *************************************************************************
    //
    // *************************************************************************

    /**
     * @return the ChronicleLoggerFactory singleton
     */
    protected ChronicleLoggerFactory getChronicleLoggerFactory() {
        return (ChronicleLoggerFactory) StaticLoggerBinder.getSingleton().getLoggerFactory();
    }

    /**
     * @param type
     * @param id
     * @return
     */
    protected IndexedChronicle getIndexedChronicle(String type, String id) throws IOException {
        return new IndexedChronicle(basePath(type, id));
    }

    /**
     * @param type
     * @param id
     * @return
     */
    protected VanillaChronicle getVanillaChronicle(String type, String id) throws IOException {
        return new VanillaChronicle(basePath(type, id));
    }

    // *************************************************************************
    //
    // *************************************************************************

    protected final static class MySerializableData implements Serializable {
        private final Object data;

        public MySerializableData(Object data) {
            this.data = data;
        }

        @Override
        public String toString() {
            return this.data.toString();
        }
    }

    protected final static class MyMarshallableData implements BytesMarshallable {
        private Object data;

        public MyMarshallableData() {
            this(null);
        }

        public MyMarshallableData(Object data) {
            this.data = data;
        }

        @Override
        public void readMarshallable(@NotNull Bytes in) throws IllegalStateException {
            this.data = in.readObject();
        }

        @Override
        public void writeMarshallable(@NotNull Bytes out) {
            out.writeObject(data);
        }

        @Override
        public String toString() {
            return this.data.toString();
        }
    }

    protected final class RunnableChronicle implements Runnable {
        private final Logger logger;
        private final int runs;
        private final String msg;

        public RunnableChronicle(int runs, int size, String loggerName) {
            this.logger = LoggerFactory.getLogger(loggerName);
            this.runs = runs;
            this.msg = StringUtils.rightPad("", size, "X");
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < this.runs; i++) {
                    this.logger.info("{},{}", this.msg, i);
                }
            } catch (Exception e) {
                this.logger.warn("Exception", e);
            }
        }
    }
}
