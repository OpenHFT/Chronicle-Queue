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

package net.openhft.chronicle.sandbox;

import net.openhft.chronicle.Excerpt;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.tools.ChronicleTools;
import net.openhft.chronicle.tools.DailingRollingIndexReader;
import net.openhft.chronicle.tools.DailingRollingReader;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.assertEquals;

/**
 * @author peter.lawrey
 */
public class DailyRollingChronicleTest {

    public static final int SIZE = 450;

    @Test
    public void testCheckIndex() throws IOException {
        File tempDir = new File(System.getProperty("java.io.tmpdir") + "/deleteme" + Long.toString(System.nanoTime(), 36));
        ChronicleTools.deleteDirOnExit(tempDir.toString());
        tempDir.mkdirs();
        String yyyyMMdd = new SimpleDateFormat("yyyyMMdd").format(new Date());

        FileOutputStream fos = new FileOutputStream(tempDir + "/" + yyyyMMdd + ".data");
        ByteBuffer bb = ByteBuffer.allocateDirect(10000).order(ByteOrder.nativeOrder());
        int pos = 0;
        for (int i = 0; i < SIZE; i++) {
            int size = 16 + i % 5;
            bb.putInt(pos, size);
            pos += (size + 3) & ~3;
        }
        bb.limit(pos);
        fos.getChannel().write(bb);
        fos.close();

        DailyRollingConfig config = new DailyRollingConfig();
        DailyRollingChronicle chronicle = new DailyRollingChronicle(tempDir.getAbsolutePath(), config);
        Assert.assertEquals(tempDir.getName() + ": 1\n"
                + "\t0\t" + yyyyMMdd + "\n",
                DailingRollingIndexReader.masterToString(tempDir).replace("\r", ""));
        assertEquals(SIZE, chronicle.size());
        chronicle.close();

        // check the index is rebuilt.
        FileInputStream fis = new FileInputStream(tempDir + "/" + yyyyMMdd + ".index");
        ByteBuffer bb2 = fis.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, fis.getChannel().size()).order(ByteOrder.nativeOrder());
        int pos2 = 0;
        for (int i = 0; i < SIZE; i++) {
            if ((i & 7) == 0) {
                long pos3 = bb2.getLong();
                Assert.assertEquals(pos2, pos3);
            }
            int size = 16 + i % 5;
            pos += (size + 3) & ~3;
        }
        fis.close();
    }

    @Test
    @Ignore
    public void writeOneAtATime() throws IOException {
        File tempDir = new File(System.getProperty("java.io.tmpdir") + "/deleteme" + Long.toString(System.nanoTime(), 36));
        ChronicleTools.deleteDirOnExit(tempDir.toString());
        tempDir.mkdirs();
        String yyyyMMdd = new SimpleDateFormat("yyyyMMdd").format(new Date());

        DailyRollingConfig config = new DailyRollingConfig();
        DailyRollingChronicle chronicle = new DailyRollingChronicle(tempDir.getAbsolutePath(), config);
        ExcerptAppender appender = chronicle.createAppender();

        DailyRollingChronicle chronicle2 = new DailyRollingChronicle(tempDir.getAbsolutePath(), config);
        Excerpt excerpt = chronicle2.createExcerpt();
        ExcerptTailer tailer = chronicle2.createTailer();

        for (int index = 0; index < 100; index++) {
            appender.startExcerpt();
            appender.writeInt(~index);
            for (int i = 0; i < index; i++)
                appender.write(-1);
            appender.finish();

            DailingRollingReader.dumpData(tempDir + "/" + yyyyMMdd + ".data", new PrintWriter(System.out));
            Assert.assertTrue(excerpt.index(index));
            Assert.assertEquals(index + 4, excerpt.size());
            Assert.assertEquals(~index, excerpt.readInt());
            Assert.assertEquals(index, excerpt.remaining());
            excerpt.finish();

            Assert.assertTrue(tailer.nextIndex());
            Assert.assertEquals(index + 4, tailer.size());
            Assert.assertEquals(~index, tailer.readInt());
            Assert.assertEquals(index, tailer.remaining());
            tailer.finish();

            // check we can jump to this index.
            tailer.index(index - 1);
            Assert.assertTrue(tailer.nextIndex());
            Assert.assertEquals(~index, tailer.readInt());
            Assert.assertEquals(index, tailer.remaining());
            tailer.finish();
        }
        chronicle2.close();
        chronicle.close();
    }
}
