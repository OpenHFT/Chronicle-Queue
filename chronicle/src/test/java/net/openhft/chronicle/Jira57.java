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

import net.openhft.chronicle.tools.ChronicleTools;
import org.junit.Assert;

import java.io.IOException;

/**
 * Created by Slawomir on 02/08/2014.
 */
public class Jira57 {

    public static class TickDataTest {
        public final long timeStamp;
        public final double price;
        public final double volume;
        public final int priceType;

        public TickDataTest(long timeStamp, double price, double volume, int priceType) {
            this.timeStamp = timeStamp;
            this.price = price;
            this.volume = volume;
            this.priceType = priceType;
        }
    }

    public static void write(TickDataTest tickData, ExcerptAppender toAppender) {
        toAppender.startExcerpt(28); // errors with, or with 28, but at a different point.
        toAppender.writeLong(tickData.timeStamp);
        toAppender.writeDouble(tickData.price);
        toAppender.writeDouble(tickData.volume);
        toAppender.writeInt(tickData.priceType);
        toAppender.finish();
    }

    public static TickDataTest readTickData(ExcerptTailer tailer) {
        long timeStamp = tailer.readLong();
        double price = tailer.readDouble();
        double volume = tailer.readDouble();
        int type = tailer.readInt();
        tailer.finish();
        return new TickDataTest(timeStamp, price, volume, type);
    }

    public static void main(String[] args) throws IOException {
        String dir = System.getProperty("java.io.tmpdir");
        String basePath = dir + "/test123";
        ChronicleTools.deleteOnExit(basePath);
        //ChronicleConfig chronicleConfig = ChronicleConfig.SMALL;   // 599168, 917501
//        ChronicleConfig chronicleConfig = ChronicleConfig.MEDIUM;   // 4793472
//        ChronicleConfig chronicleConfig = ChronicleConfig.LARGE;   // 19173960
        Chronicle writeChronicle = ChronicleQueueBuilder.indexed(basePath).small().build();
//        VanillaChronicleConfig chronicleConfig = VanillaChronicleConfig.DEFAULT;
//        VanillaChronicle writeChronicle = new VanillaChronicle(basePath, chronicleConfig);
        ExcerptAppender appender = writeChronicle.createAppender();
        int numberOfTicks = 20000000;
        long startDate = 1423432423;
        long[] indexes = new long[numberOfTicks];
        for (int i = 0; i < numberOfTicks; i++) {
            TickDataTest td = new TickDataTest(startDate + i * 10L, i, i, 1);
            write(td, appender);
            indexes[i] = appender.lastWrittenIndex();
        }
        writeChronicle.close();

        Chronicle readChronicle = ChronicleQueueBuilder.indexed(basePath).small().build();
//        VanillaChronicle readChronicle = new VanillaChronicle(basePath, chronicleConfig);
        Excerpt excerpt = readChronicle.createExcerpt();
        ExcerptTailer tailer = readChronicle.createTailer();
        for (int start = 0; start < numberOfTicks - 3; start++) {
            Assert.assertTrue(excerpt.index(indexes[start]));
            Assert.assertTrue(tailer.index(indexes[start]));

//            Assert.assertEquals(dump(excerpt), dump(tailer));
            for (int j = 1; j <= 3; j++) {
                int count = start + j;

                Assert.assertTrue(excerpt.nextIndex());
                Assert.assertFalse(excerpt.wasPadding());
                boolean condition = tailer.nextIndex();
                if (j == 3)
                Assert.assertEquals(dump(excerpt), dump(tailer));
                if (!condition)
                    Assert.assertTrue("start: " + start + ", j: " + j, condition);
                Assert.assertFalse(tailer.wasPadding());

                if (excerpt.remaining() != 28)
                    Assert.assertEquals("start: " + start + " excerpt.index: " + excerpt.index(), 28, excerpt.remaining());
                if (tailer.remaining() != 28)
                    Assert.assertEquals("start: " + start + " tailer.index: " + tailer.index(), 28, tailer.remaining());
                try {
                    TickDataTest td2 = readTickData(excerpt);
                    if (count != td2.price)
                        Assert.assertEquals("index: " + excerpt.index(), count, td2.price, 0);
                    Assert.assertEquals(count, td2.volume, 0);
                    TickDataTest td = readTickData(tailer);
                    if (count != td.price)
                        Assert.assertEquals("index: " + tailer.index(), count, td.price, 0);
                    Assert.assertEquals(count, td.volume, 0);
                } catch (Exception e) {
                    System.err.println("Failed at index: " + tailer.index());
                    throw e;
                }
            }
        }
        readChronicle.close();
    }

    static String dump(ExcerptCommon common) {
        return ((IndexedChronicle.AbstractIndexedExcerpt) common).dumpState();
    }
}