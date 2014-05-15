/*
 * Copyright 2013 Peter Lawrey
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

package net.openhft.chronicle;

import net.openhft.chronicle.tcp.VanillaChronicleSink;
import net.openhft.chronicle.tcp.VanillaChronicleSource;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class VanillaChronicleSourceTest {
    @Test
    public void testReplication() throws IOException {
        int RUNS = 100;

        String basePath = System.getProperty("java.io.tmpdir") + "/tmp/testReplication";
        VanillaChronicleSource chronicle = new VanillaChronicleSource(new VanillaChronicle(basePath + "-source"), 0);
        int localPort = chronicle.getLocalPort();
        VanillaChronicleSink chronicle2 = new VanillaChronicleSink(new VanillaChronicle(basePath + "-sink"), "localhost", localPort);

        try {
            ExcerptAppender appender = chronicle.createAppender();
            ExcerptTailer tailer = chronicle2.createTailer();

            for (int i = 0; i < RUNS; i++) {
                appender.startExcerpt();
                long value = 1000000000 + i;
                appender.append(value).append(' ');
                appender.finish();
                while(true){
                  if(tailer.nextIndex())break;
                }

                long val = tailer.parseLong();
                System.out.println(val);
                Assert.assertEquals("i: " + i, value, val);
                Assert.assertEquals("i: " + i, 0, tailer.remaining());
                tailer.finish();

            }
        } finally {
            chronicle2.close();
            chronicle.close();
            chronicle2.clear();
            chronicle.clear();
        }
    }


    @Test
    public void testReplication2() throws IOException {
        int RUNS = 100;

        String basePath = System.getProperty("java.io.tmpdir") + "/tmp/testReplication2";
        VanillaChronicleSource chronicle = new VanillaChronicleSource(new VanillaChronicle(basePath + "-source"), 0);
        int localPort = chronicle.getLocalPort();
        VanillaChronicleSink chronicle2 = new VanillaChronicleSink(new VanillaChronicle(basePath + "-sink"), "localhost", localPort);

        try {
            ExcerptAppender appender = chronicle.createAppender();

            for (int i = 0; i < RUNS; i++) {
                appender.startExcerpt();
                long value = 1000000000 + i;
                appender.append(value).append(' ');
                appender.finish();
            }

            ExcerptTailer tailer = chronicle2.createTailer();
            for (int i = 0; i < RUNS; i++) {
                long value = 1000000000 + i;
                boolean nextIndex = tailer.nextIndex();
                long val = tailer.parseLong();
                System.out.println(val);
                Assert.assertEquals("i: " + i, value, val);
                Assert.assertEquals("i: " + i, 0, tailer.remaining());
                tailer.finish();

            }

        } finally {
            chronicle2.close();
            chronicle.close();
            chronicle2.clear();
            chronicle.clear();
        }
    }

    @Test
    public void testReplicationWithRolling() throws Exception {
        int RUNS = 500;

        String basePath = System.getProperty("java.io.tmpdir") + "/tmp/testReplicationWithRolling";
        VanillaChronicleConfig config = new VanillaChronicleConfig();
        config.entriesPerCycle(1L << 20);
        config.cycleLength(1000, false);
        config.cycleFormat("yyyyMMddHHmmss");
        config.indexBlockSize(16L << 10);
        VanillaChronicleSource chronicle = new VanillaChronicleSource(new VanillaChronicle(basePath + "-source", config), 0);

        int localPort = chronicle.getLocalPort();
        VanillaChronicleSink chronicle2 = new VanillaChronicleSink(new VanillaChronicle(basePath + "-sink", config), "localhost", localPort);

        try {
            ExcerptAppender appender = chronicle.createAppender();
            ExcerptTailer tailer = chronicle2.createTailer();

            for (int i = 0; i < RUNS; i++) {
                appender.startExcerpt();
                long value = 1000000000 + i;
                appender.append(value).append(' ');
                appender.finish();
                Thread.sleep(10);

                while(true){
                    if(tailer.nextIndex())break;
                }
                Assert.assertEquals("i: " + i, value, tailer.parseLong());
                Assert.assertEquals("i: " + i, 0, tailer.remaining());
                tailer.finish();
            }
        } finally {
            chronicle2.close();
            chronicle.close();
            chronicle2.clear();
            chronicle.clear();
        }
    }


    @Test
    public void testReplicationWithRolling2() throws Exception {
        int RUNS = 100;

        String basePath = System.getProperty("java.io.tmpdir") + "/tmp/testReplicationWithRolling2";
        VanillaChronicleConfig config = new VanillaChronicleConfig();
        config.entriesPerCycle(1L << 20);
        config.cycleLength(1000, false);
        config.cycleFormat("yyyyMMddHHmmss");
        config.indexBlockSize(16L << 10);
        VanillaChronicleSource chronicle = new VanillaChronicleSource(new VanillaChronicle(basePath + "-source", config), 55555);
        VanillaChronicleSink chronicle2 = new VanillaChronicleSink(new VanillaChronicle(basePath + "-sink", config), "localhost", 55555);

        try {
            ExcerptAppender appender = chronicle.createAppender();
            ExcerptTailer tailer = chronicle2.createTailer();
            for (int i = 0; i < RUNS; i++) {
                appender.startExcerpt();
                long value = 1000000000 + i;
                appender.append(value).append(' ');
                appender.finish();
                Thread.sleep(100);

                while(true){
                    if(tailer.nextIndex())break;
                }
                long val = tailer.parseLong();
                Assert.assertEquals("i: " + i, value, val);
                Assert.assertEquals("i: " + i, 0, tailer.remaining());
                tailer.finish();
            }
        } finally {
            chronicle2.close();
            chronicle.close();
            chronicle2.clear();
            chronicle.clear();
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
    @Test
    public void testSourceSinkStartResumeRollingEverySecond() throws Exception {
        //This is the config that is required to make the VanillaChronicle roll every second
        final VanillaChronicleConfig config = new VanillaChronicleConfig();
        config.entriesPerCycle(1L << 20);
        config.cycleLength(1000, false);
        config.cycleFormat("yyyyMMddHHmmss");
        config.indexBlockSize(16L << 10);

        final String baseDir_source = System.getProperty("java.io.tmpdir") + "/tmp/testAppendRolling_Source";
        final String baseDir_sink = System.getProperty("java.io.tmpdir") + "/tmp/testAppendRolling_Sink";

        final VanillaChronicle chronicle_source = new VanillaChronicle(baseDir_source, config);
        final VanillaChronicleSource source = new VanillaChronicleSource(chronicle_source, 8888);

        new Thread(){
            public void run(){
                try{
                    ExcerptAppender appender = source.createAppender();
                    System.out.print("writing 100 items will take take 10 seconds.");
                    for (int i = 0; i < 100; i++) {
                        appender.startExcerpt();
                        int value = 1000000000 + i;
                        appender.append(value).append(' '); //this space is really important.
                        appender.finish();
                        Thread.sleep(100);
                        if(i % 10==0)System.out.print(".");
                    }
                    System.out.print("\n");
                }catch(Exception e){
                    e.printStackTrace();
                }
                finally {
                    //keep the thread alive so that the sinks can connect to the source
                    try {
                        Thread.sleep(3000 * 1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }.start();

        //wait for the appender to write all the entries
        try {
            Thread.sleep(11 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //create a tailer to get the first 50 items then exit the tailer
        VanillaChronicle chronicle_sink = new VanillaChronicle(baseDir_sink, config);
        VanillaChronicleSink sink = new VanillaChronicleSink(chronicle_sink, "localhost", 8888);
        ExcerptTailer tailer = sink.createTailer();

        int count=0;
        System.out.println("Sink reading first 50 items then stopping");
        while (true) {
            if(tailer.nextIndex()){
                long ll = tailer.parseLong();
                int value = 1000000000 + count;
                Assert.assertEquals(value, ll);
                tailer.finish();
                count ++;
                if(count == 50)break;
            }
        }
        sink.close();

        //now resume the tailer to get the first 50 items
        chronicle_sink = new VanillaChronicle(baseDir_sink, config);
        sink = new VanillaChronicleSink(chronicle_sink, "localhost", 8888);
        tailer = sink.createTailer();
        //Take the tailer to the last index (item 50) and start reading from there.
        tailer.toEnd();

        System.out.println("Sink restarting to continue to read the next 50 items");
        while (true) {
            if(tailer.nextIndex()){
                long ll = tailer.parseLong();
                tailer.finish();
                int value = 1000000000 + count;
                Assert.assertEquals(value, ll);
                count ++;
                if(count == 100)break;
            }
        }

        source.close();
        source.clear();
        sink.close();
        sink.clear();
    }
}
