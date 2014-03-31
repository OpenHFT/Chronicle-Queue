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

package net.openhft.chronicle.sandbox.tcp;

import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.VanillaChronicle;
import net.openhft.chronicle.VanillaChronicleConfig;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

@Ignore
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
        config.cycleLength(1000);
        config.cycleFormat("yyyyMMddHHmmss");
        config.entriesPerCycle(1L << 20);
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

                tailer.nextIndex();
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
        config.cycleLength(1000);
        config.cycleFormat("yyyyMMddHHmmss");
        config.entriesPerCycle(1L << 20);
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

                tailer.nextIndex();
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
}
