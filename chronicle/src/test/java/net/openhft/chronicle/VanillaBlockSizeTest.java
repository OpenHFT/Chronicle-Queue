/*
 * Copyright 2015 Higher Frequency Trading
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

import net.openhft.lang.Jvm;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author luke
 *         Date: 4/7/15
 */
//@Ignore
public class VanillaBlockSizeTest extends VanillaChronicleTestBase {

    @Rule
    public final TemporaryFolder tmpdir = new TemporaryFolder(new File(System.getProperty("java.io.tmpdir")));

    @Rule
    public final TestName testName = new TestName();

    @Ignore
    @Test
    public void testMaxSize() throws IOException {
        final File root = tmpdir.newFolder(testName.getMethodName());
        root.deleteOnExit();

        final int SAMPLE = 1000000; // 10000000
        final int ITEM_SIZE = 8; // 256

        byte[] byteArrays = new byte[ITEM_SIZE];
        new Random().nextBytes(byteArrays);

        Chronicle chronicle = ChronicleQueueBuilder
            .vanilla(root.getAbsolutePath() + "/blocksize-test")
            .dataBlockSize(1073741824)
            .indexBlockSize(1073741824)
            .build();

        ExcerptAppender appender = chronicle.createAppender();

        System.out.println("Test...");
        for (int count=0 ; ; count++) {
            appender.startExcerpt(ITEM_SIZE);
            appender.write(byteArrays, 0, ITEM_SIZE);
            appender.finish();

            if (count != 0 && count % SAMPLE == 0) {
                System.out.println(count + " written");
            }
        }
    }

    final int MAX_SIZE_TAIL_SAMPLE = 10000000; // 10000000
    final int MAX_SIZE_TAIL_ITEM_SIZE = 256;
    final int MAX_SIZE_TAIL_ITEMS = Integer.MAX_VALUE;

    @Ignore
    @Test
    public void testMaxSizeTail() throws IOException, InterruptedException {
        final File root = tmpdir.newFolder(testName.getMethodName());
        root.deleteOnExit();

        final ChronicleQueueBuilder builder = ChronicleQueueBuilder.vanilla(root)
                .dataBlockSize(1073741824)
                .indexBlockSize(1073741824);

        final int appenders = 3;
        final int tailers = 1;

        final ExecutorService executor = Executors.newFixedThreadPool(appenders + tailers);
        for(int i=0; i<appenders; i++) {
            executor.execute(new MaxSizeAppender(builder, "p-" + i));
        }
        for(int i=0; i<tailers; i++) {
            executor.execute(new MaxSizeTailer(builder, "t-" + i));
        }

        executor.awaitTermination(30, TimeUnit.MINUTES);
    }

    // *************************************************************************
    // Helpers
    // *************************************************************************

    private class MaxSizeAppender implements Runnable {
        private final String id;
        private final ChronicleQueueBuilder builder;

        public MaxSizeAppender(final ChronicleQueueBuilder builder, String id) {
            this.builder = builder;
            this.id = id;
        }

        public void run() {
            try {
                Chronicle chronicle = this.builder.build();
                byte[] byteArrays = new byte[MAX_SIZE_TAIL_ITEM_SIZE];
                new Random().nextBytes(byteArrays);

                final ExcerptAppender appender = chronicle.createAppender();
                for (int count = 0; count < MAX_SIZE_TAIL_ITEMS; count++) {
                    appender.startExcerpt(MAX_SIZE_TAIL_ITEM_SIZE);
                    appender.write(byteArrays, 0, MAX_SIZE_TAIL_ITEM_SIZE);
                    appender.finish();

                    if (count != 0 && count % MAX_SIZE_TAIL_SAMPLE == 0) {
                        LOGGER.info(id + ": " + count + " written");
                    }
                }

                appender.close();
            } catch (Exception e) {
                LOGGER.error("", e);
                errorCollector.addError(e);
            } catch (AssertionError a) {
                LOGGER.error("", a);
                errorCollector.addError(a);
            }
        }
    }

    private class MaxSizeTailer implements Runnable {
        private final String id;
        private final ChronicleQueueBuilder builder;

        public MaxSizeTailer(final ChronicleQueueBuilder builder, String id) {
            this.builder = builder;
            this.id = id;
        }

        public void run() {
            try {
                Chronicle chronicle = this.builder.build();
                byte[] byteArrays = new byte[MAX_SIZE_TAIL_ITEM_SIZE];

                final ExcerptTailer tailer = chronicle.createTailer();
                for (int count = 0; count < MAX_SIZE_TAIL_ITEMS; ) {
                    if(tailer.nextIndex()) {
                        tailer.read(byteArrays);
                        tailer.finish();

                        if (count != 0 && count % MAX_SIZE_TAIL_SAMPLE == 0) {
                            LOGGER.info(this.id + ": " + count + " read");
                        }

                        count++;
                    }
                }

                tailer.close();
            } catch (Exception e) {
                LOGGER.error("", e);
                errorCollector.addError(e);
            } catch (AssertionError a) {
                LOGGER.error("", a);
                errorCollector.addError(a);
            }
        }
    }
}
