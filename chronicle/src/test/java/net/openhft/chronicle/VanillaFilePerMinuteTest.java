/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle;

import net.openhft.lang.io.IOTools;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class VanillaFilePerMinuteTest extends VanillaChronicleTestBase {

    static final String FMT = "yyyyMMddHHmm";
    static final int    MIN = 60 * 1000;

    @Test
    public void filePerMinuteTest() throws Exception {
        final String basePath = getTestPath();
        IOTools.deleteDir(basePath);

        LOGGER.info("Basepath is {}", basePath);

        final ExecutorService svc = Executors.newFixedThreadPool(2);
        svc.execute(createWriter(createChronicle(basePath)));
        svc.execute(createReader(createChronicle(basePath)));
        svc.awaitTermination(10, TimeUnit.MINUTES);
    }

    // *************************************************************************
    //
    // *************************************************************************

    static Runnable createReader(final Chronicle chron) {
        return new Runnable() {
            @Override
            public void run() {
                try(ExcerptTailer tailer = chron.createTailer()) {
                    for(int i=0; i<1000;) {
                        if(tailer.nextIndex()) {
                            LOGGER.info("> R : {}", tailer.readInt());
                            i++;
                        } else {
                            Thread.sleep(1000);
                        }
                    }
                } catch (Exception e) {
                    LOGGER.warn("", e);
                }
            }
        };
    }

    static Runnable createWriter(final Chronicle chron) {
        return new Runnable() {
            @Override
            public void run() {
                try(ExcerptAppender appender = chron.createAppender()) {
                    for (int i = 0; i < 1000; i++) {
                        LOGGER.info("> W : {}", i);

                        appender.startExcerpt(4);
                        appender.writeInt(i);
                        appender.finish();

                        Thread.sleep(1000);
                    }
                } catch (Exception e) {
                    LOGGER.warn("", e);
                }
            }
        };
    }

    static Chronicle createChronicle(String basePath) throws IOException {
        return ChronicleQueueBuilder.vanilla(basePath)
            .cycleLength(MIN, false)
            .cycleFormat(FMT)
            .build();
    }
}
