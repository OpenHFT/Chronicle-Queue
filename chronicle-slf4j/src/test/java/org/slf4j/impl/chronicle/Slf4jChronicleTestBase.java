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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 */
public class Slf4jChronicleTestBase {

    // *************************************************************************
    //
    // *************************************************************************

    protected static String basePath(String type,String loggerName) {
        return System.getProperty("java.io.tmpdir")
            + File.separator
            + "chronicle"
            + File.separator
            + type
            + File.separator
            + new SimpleDateFormat("yyyyMMdd").format(new Date())
            + File.separator
            + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]
            + File.separator
            + loggerName;
    }

    // *************************************************************************
    //
    // *************************************************************************

    /**
     *
     */
    protected final class RunnableChronicle implements Runnable {
        private final Logger logger;
        private final int runs;

        /**
         *
         * @param runs
         * @param loggerName
         */
        public RunnableChronicle(int runs,String loggerName) {
            this.logger = LoggerFactory.getLogger(loggerName);
            this.runs = runs;
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < this.runs; i++) {
                    this.logger.info("runLoop {}",i);
                    Thread.yield();
                }
            } catch (Exception e) {
                this.logger.warn("Exception",e);
            }
        }
    }
}
