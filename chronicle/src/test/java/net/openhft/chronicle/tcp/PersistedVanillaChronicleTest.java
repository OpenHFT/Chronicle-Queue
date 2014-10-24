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

package net.openhft.chronicle.tcp;

import net.openhft.chronicle.*;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class PersistedVanillaChronicleTest extends PersistedChronicleTestBase {

    @Test
    public void testReplication1() throws Exception {
        final int RUNS = 100;

        final String sourceBasePath = getVanillaTestPath("-source");
        final String sinkBasePath = getVanillaTestPath("-sink");

        final ChronicleSource source = new ChronicleSource(
            new VanillaChronicle(sourceBasePath), 0);
        final ChronicleSink sink = new ChronicleSink(
            new VanillaChronicle(sinkBasePath), "localhost", source.getLocalPort());

        try {

            final Thread at = new Thread("th-appender") {
                public void run() {
                    try {
                        final ExcerptAppender appender = source.createAppender();
                        for (int i = 0; i < RUNS; i++) {
                            appender.startExcerpt();
                            long value = 1000000000 + i;
                            appender.append(value).append(' ');
                            appender.finish();
                        }

                        appender.close();
                    } catch(Exception e) {
                    }
                }
            };

            final Thread tt = new Thread("th-tailer") {
                public void run() {
                    try {
                        final ExcerptTailer tailer = sink.createTailer();
                        for (int i = 0; i < RUNS; i++) {
                            long value = 1000000000 + i;
                            assertTrue(tailer.nextIndex());
                            long val = tailer.parseLong();

                            assertEquals("i: " + i, value, val);
                            assertEquals("i: " + i, 0, tailer.remaining());
                            tailer.finish();
                        }

                        tailer.close();
                    } catch(Exception e) {
                    }
                }
            };

            at.start();
            tt.start();

            at.join();
            tt.join();
        } finally {
            sink.close();
            sink.clear();

            source.close();
            source.clear();

            assertFalse(new File(sourceBasePath).exists());
            assertFalse(new File(sinkBasePath).exists());
        }
    }

    @Test
    public void testReplicationWithRolling1() throws Exception {
        final int RUNS = 500;

        final String sourceBasePath = getVanillaTestPath("-source");
        final String sinkBasePath = getVanillaTestPath("-sink");

        final VanillaChronicleConfig config = new VanillaChronicleConfig()
            .entriesPerCycle(1L << 20)
            .cycleLength(1000, false)
            .cycleFormat("yyyyMMddHHmmss")
            .indexBlockSize(16L << 10);

        final ChronicleSource source = new ChronicleSource(
            new VanillaChronicle(sourceBasePath, config), 0);
        final ChronicleSink sink = new ChronicleSink(
            new VanillaChronicle(sinkBasePath, config), "localhost", source.getLocalPort());

        try {
            final Thread at = new Thread("th-appender") {
                public void run() {
                    try {
                        final ExcerptAppender appender = source.createAppender();
                        for (int i = 0; i < RUNS; i++) {
                            appender.startExcerpt();

                            long value = 1000000000 + i;
                            appender.append(value).append(' ');
                            appender.finish();

                            sleep(10);
                        }

                        appender.close();
                    } catch(Exception e) {
                    }
                }
            };

            final Thread tt = new Thread("th-tailer") {
                public void run() {
                    try {
                        final ExcerptTailer tailer = sink.createTailer();
                        for (int i = 0; i < RUNS; i++) {
                            while (!tailer.nextIndex()) ;

                            long value = 1000000000 + i;
                            assertEquals("i: " + i, value, tailer.parseLong());
                            assertEquals("i: " + i, 0, tailer.remaining());

                            tailer.finish();
                        }

                        tailer.close();
                    } catch(Exception e) {
                    }
                }
            };

            at.start();
            tt.start();

            at.join();
            tt.join();
        } finally {
            sink.close();
            sink.clear();

            source.close();
            source.clear();

            assertFalse(new File(sourceBasePath).exists());
            assertFalse(new File(sinkBasePath).exists());
        }
    }

    @Test
    public void testReplicationWithRolling2() throws Exception {
        final int RUNS = 100;

        final String sourceBasePath = getVanillaTestPath("-source");
        final String sinkBasePath = getVanillaTestPath("-sink");

        final VanillaChronicleConfig config = new VanillaChronicleConfig()
            .entriesPerCycle(1L << 20)
            .cycleLength(1000, false)
            .cycleFormat("yyyyMMddHHmmss")
            .indexBlockSize(16L << 10);

        final ChronicleSource source = new ChronicleSource(
            new VanillaChronicle(sourceBasePath, config), 0);
        final ChronicleSink sink = new ChronicleSink(
            new VanillaChronicle(sinkBasePath, config), "localhost", source.getLocalPort());

        try {
            final Thread at = new Thread("th-appender") {
                public void run() {
                    try {
                        final ExcerptAppender appender = source.createAppender();
                        for (int i = 0; i < RUNS; i++) {
                            appender.startExcerpt();

                            long value = 1000000000 + i;
                            appender.append(value).append(' ');
                            appender.finish();

                            sleep(100);
                        }

                        appender.close();
                    } catch(Exception e) {
                    }
                }
            };

            final Thread tt = new Thread("th-tailer") {
                public void run() {
                    try {
                        final ExcerptTailer tailer = sink.createTailer();
                        for (int i = 0; i < RUNS; i++) {
                            while (!tailer.nextIndex()) ;

                            long value = 1000000000 + i;
                            assertEquals("i: " + i, value, tailer.parseLong());
                            assertEquals("i: " + i, 0, tailer.remaining());

                            tailer.finish();
                        }

                        tailer.close();
                    } catch(Exception e) {
                    }
                }
            };

            at.start();
            tt.start();

            at.join();
            tt.join();
        } finally {
            sink.close();
            sink.clear();

            source.close();
            source.clear();

            assertFalse(new File(sinkBasePath).exists());
            assertFalse(new File(sourceBasePath).exists());
        }
    }

    /**
     * This test tests the following functionality.
     * (1) It ensures that data can be written to a VanillaChronicle Source over a period of 10 seconds
     * whilst the chronicle is rolling files every second.
     * (2) It also ensures that the Sink can be tailed to fetch the items from that Source.
     * (3) Critically it ensures that even though the Sink is stopped and then restarted it resumes
     * from the index at which it stopped.
     */
    //@Ignore // need to investigate why toEnd does not find the righ message
    @Test
    public void testSourceSinkStartResumeRollingEverySecond() throws Exception {
        //This is the config that is required to make the VanillaChronicle roll every second
        final VanillaChronicleConfig config = new VanillaChronicleConfig();
        config.entriesPerCycle(1L << 20);
        config.cycleLength(1000, false);
        config.cycleFormat("yyyyMMddHHmmss");
        config.indexBlockSize(16L << 10);

        final String sourceBasePath = getVanillaTestPath("-source");
        final String sinkBasePath = getVanillaTestPath("-sink");
        assertNotNull(sourceBasePath);
        assertNotNull(sinkBasePath);

        final ChronicleSource source = new ChronicleSource(new VanillaChronicle(sourceBasePath, config), 8888);

        ExcerptAppender appender = source.createAppender();
        System.out.print("writing 100 items will take take 10 seconds.");
        for (int i = 0; i < 100; i++) {
            appender.startExcerpt();
            int value = 1000000000 + i;
            appender.append(value).append(' '); //this space is really important.
            appender.finish();
            Thread.sleep(100);

            if(i % 10==0) {
                System.out.print(".");
            }
        }

        appender.close();
        System.out.print("\n");

        //create a tailer to get the first 50 items then exit the tailer
        final ChronicleSink sink1 = new ChronicleSink(new VanillaChronicle(sinkBasePath, config), "localhost", 8888);
        final ExcerptTailer tailer1 = sink1.createTailer().toStart();

        System.out.println("Sink1 reading first 50 items then stopping");
        for( int count=0; count < 50 ;) {
            if(tailer1.nextIndex()) {
                assertEquals(1000000000 + count, tailer1.parseLong());
                tailer1.finish();

                count++;
            }
        }

        tailer1.close();
        sink1.close();
        sink1.checkCounts(1, 1);

        //now resume the tailer to get the first 50 items
        final ChronicleSink sink2 = new ChronicleSink(new VanillaChronicle(sinkBasePath, config), "localhost", 8888);

        //Take the tailer to the last index (item 50) and start reading from there.
        final ExcerptTailer tailer2 = sink2.createTailer().toEnd();
        assertEquals(1000000000 + 49, tailer2.parseLong());
        tailer2.finish();
        
        System.out.println("Sink2 restarting to continue to read the next 50 items");
        for(int count=50 ; count < 100 ; ) {
            if(tailer2.nextIndex()) {
                assertEquals(1000000000 + count, tailer2.parseLong());
                tailer2.finish();

                count++;
            }
        }

        tailer2.close();
        sink2.close();
        sink2.checkCounts(1, 1);

        sink2.clear();

        source.close();
        source.checkCounts(1, 1);
        source.clear();

        assertFalse(new File(sourceBasePath).exists());
        assertFalse(new File(sinkBasePath).exists());
    }
}
