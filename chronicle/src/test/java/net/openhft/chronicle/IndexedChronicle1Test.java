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

import net.openhft.chronicle.tools.ChronicleIndexReader;
import net.openhft.lang.model.constraints.Nullable;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * @author peter.lawrey
 */
public class IndexedChronicle1Test extends IndexedChronicleTestBase {

    protected static void assertEquals(long a, long b) {
        if (a != b) {
            Assert.assertEquals(a, b);
        }
    }

    protected static <T> void assertEquals(@Nullable T a, @Nullable T b) {
        if (a == null) {
            if (b == null) {
                return;
            }
        } else if (a.equals(b)) {
            return;
        }

        Assert.assertEquals(a, b);
    }

    // *************************************************************************
    //
    // *************************************************************************

    @Test
    public void testSerializationPerformance() throws IOException, ClassNotFoundException, InterruptedException {
        final String basePath = getTestPath();
        final Chronicle chronicle = ChronicleQueueBuilder.indexed(basePath).build();

        try {
            ExcerptAppender appender = chronicle.createAppender();
            int objects = 1000000;
            long start = System.nanoTime();
            for (int i = 0; i < objects; i++) {
                appender.startExcerpt();
                appender.writeObject(BigDecimal.valueOf(i % 1000));
                appender.finish();
            }

            appender.close();

            ExcerptTailer tailer = chronicle.createTailer();
            for (int i = 0; i < objects; i++) {
                assertTrue(tailer.nextIndex() || tailer.nextIndex());
                BigDecimal bd = (BigDecimal) tailer.readObject();
                assertEquals(i % 1000, bd.longValue());
                tailer.finish();
            }

            tailer.close();

            //        System.out.println("waiting");
            //        Thread.sleep(20000);
            //        System.out.println("waited");
            //        System.gc();
            long time = System.nanoTime() - start;
            System.out.printf("The average time to write and read a BigDecimal was %,d ns%n", time / objects);
            //        tsc = null;
            //        System.gc();
            //        Thread.sleep(10000);
        } finally {
            chronicle.close();
            assertClean(basePath);
        }
    }

    @Test
    @Ignore
    public void rewritibleEntries() throws IOException {
//        boolean[] booleans = {false, true};
//        for (boolean useUnsafe : booleans)
//            for (boolean minimiseByteBuffers : booleans)
//                for (boolean synchronousMode : booleans)
//                    doRewriteableEntries(useUnsafe, minimiseByteBuffers, synchronousMode);
        doRewriteableEntries(true, true, false);
    }

    private void doRewriteableEntries(boolean useUnsafe, boolean minimiseByteBuffers, boolean synchronousMode) throws IOException {
        final String basePath = getTestPath();
        final Chronicle chronicle = ChronicleQueueBuilder.indexed(basePath).build();

        try {
            ExcerptAppender excerpt = chronicle.createAppender();

            int counter = 1;
            for (int i = 0; i < 1024; i++) {
                excerpt.startExcerpt();
                for (int j = 0; j < 128; j += 8)
                    excerpt.writeLong(counter++);
                excerpt.write(-1);
                excerpt.finish();
            }

            int counter2 = 1;
            ExcerptTailer excerpt2 = chronicle.createTailer();
            while (excerpt2.nextIndex()) {
                for (int j = 0; j < 128; j += 8) {
                    long actual = excerpt2.readLong();
                    long expected = counter2++;
                    if (expected != actual)
                        assertEquals(expected, actual);
                }
                assertEquals(-1, excerpt2.readByte());
                excerpt2.finish();
            }
            assertEquals(counter, counter2);
        } finally {
            chronicle.close();
            assertClean(basePath);
        }
    }

    /**
     * Tests that <code>IndexedChronicle.close()</code> does not blow up (anymore)
     * when you reopen an existing chronicle due to the null data buffers created
     * internally.
     *
     * @throws IOException if opening chronicle fails
     */
    @Test
    public void testCloseWithNullBuffers() throws IOException {
        final String basePath = getTestPath();

        Chronicle chronicle = ChronicleQueueBuilder.indexed(basePath).build();

//        tsc.clear();
        ExcerptAppender excerpt = chronicle.createAppender();
        for (int i = 0; i < 512; i++) {
            excerpt.startExcerpt();
            excerpt.writeByte(1);
            excerpt.finish();
        }
        // used to throw NPE if you have finished already.
        excerpt.close();

        chronicle.close();

        chronicle = ChronicleQueueBuilder.indexed(basePath).build();
        chronicle.createAppender().close();
        chronicle.close(); // used to throw an exception.

        assertClean(basePath);
    }

    @Test
    @Ignore
    public void testTimeTenMillion() throws IOException {
        final String basePath = getTestPath();

        int repeats = 3;
        for (int j = 0; j < repeats; j++) {
            long start = System.nanoTime();

            int records = 10 * 1000 * 1000; {
                Chronicle ic = ChronicleQueueBuilder.indexed(basePath).build();
//                ic.useUnsafe(true);
//                ic.clear();
                ExcerptAppender excerpt = ic.createAppender();
                for (int i = 1; i <= records; i++) {
                    excerpt.startExcerpt();
                    excerpt.writeLong(i);
                    excerpt.writeDouble(i);
                    excerpt.finish();
                }
                ic.close();
            }
            {
                Chronicle ic = ChronicleQueueBuilder.indexed(basePath).build();
//                ic.useUnsafe(true);
                ExcerptTailer excerpt = ic.createTailer();
                for (int i = 1; i <= records; i++) {
                    boolean found = excerpt.nextIndex() || excerpt.nextIndex();
                    if (!found)
                        assertTrue(found);
                    long l = excerpt.readLong();
                    double d = excerpt.readDouble();
                    if (l != i)
                        assertEquals(i, l);
                    if (d != i)
                        assertEquals((double) i, d);
                    excerpt.finish();
                }
                ic.close();
            }
            long time = System.nanoTime() - start;
            System.out.printf("Time taken %,d ms%n", time / 1000000);
        }
    }

    /**
     * https://github.com/peter-lawrey/Java-Chronicle/issues/9
     *
     * @author AndrasMilassin
     */
    @Test
    public void testBoolean() throws Exception {
        final String basePath = getTestPath();
        final Chronicle chronicle = ChronicleQueueBuilder.indexed(basePath).build();

        try {
            ExcerptAppender excerpt = chronicle.createAppender();
            excerpt.startExcerpt();
            excerpt.writeBoolean(false);
            excerpt.writeBoolean(true);
            excerpt.finish();

            ExcerptTailer tailer = chronicle.createTailer();
            tailer.nextIndex();
            boolean one = tailer.readBoolean();
            boolean onetwo = tailer.readBoolean();

            Assert.assertEquals(false, one);
            Assert.assertEquals(true, onetwo);
        } finally {
            chronicle.close();
            assertClean(basePath);
        }
    }

    @Test
    public void testStopBitEncoded() throws IOException {
        boolean ok = false;
        final String basePath = getTestPath();
        final Chronicle chronicle = ChronicleQueueBuilder.indexed(basePath).build();

        try {
            ExcerptAppender writer = chronicle.createAppender();
            ExcerptTailer reader = chronicle.createTailer();
            long[] longs = {Long.MIN_VALUE, Integer.MIN_VALUE, Short.MIN_VALUE, Character.MIN_VALUE, Byte.MIN_VALUE,
                    Long.MAX_VALUE, Integer.MAX_VALUE, Short.MAX_VALUE, Character.MAX_CODE_POINT, Character.MAX_VALUE, Byte.MAX_VALUE};
            for (long l : longs) {
                writer.startExcerpt();
                writer.writeChar('T');
                writer.writeStopBit(l);
                writer.finish();
//                System.out.println("finished");

                reader.nextIndex();
                reader.readChar();
                long l2 = reader.readStopBit();
                reader.finish();
                assertEquals(l, l2);
            }
            writer.startExcerpt(longs.length * 10);
            writer.writeChar('t');
            for (long l : longs)
                writer.writeStopBit(l);
            writer.finish();
            writer.close();

            reader.nextIndex();
            reader.readChar();
            for (long l : longs) {
                long l2 = reader.readStopBit();
                assertEquals(l, l2);
            }
            assertEquals(0, reader.remaining());
            reader.finish();
            reader.close();

            ok = true;
        } finally {
            chronicle.close();

            if (!ok) {
                ChronicleIndexReader.main(basePath);
            } else {
                assertClean(basePath);
            }
        }
    }

    @Test
    public void testEnum() throws IOException {
        final String basePath = getTestPath();
        final Chronicle chronicle = ChronicleQueueBuilder.indexed(basePath).build();

        try {
            ExcerptAppender excerpt = chronicle.createAppender();
            excerpt.startExcerpt();
            excerpt.writeUTFΔ(AccessMode.EXECUTE.name());
            excerpt.writeEnum(AccessMode.EXECUTE);
            excerpt.writeEnum(AccessMode.READ);
            excerpt.writeEnum(AccessMode.WRITE);
            excerpt.writeEnum(BigInteger.ONE);
            excerpt.writeEnum(BigInteger.TEN);
            excerpt.writeEnum(BigInteger.ZERO);
            excerpt.writeEnum(BigInteger.ONE);
            excerpt.writeEnum(BigInteger.TEN);
            excerpt.writeEnum(BigInteger.ZERO);
            excerpt.finish();
            excerpt.close();

            ExcerptTailer tailer = chronicle.createTailer();
            tailer.nextIndex();
            AccessMode e = tailer.readEnum(AccessMode.class);
            String e2 = tailer.readEnum(String.class);
            assertEquals(AccessMode.EXECUTE.name(), e2);
            AccessMode r = tailer.readEnum(AccessMode.class);
            AccessMode w = tailer.readEnum(AccessMode.class);
            BigInteger one = tailer.readEnum(BigInteger.class);
            BigInteger ten = tailer.readEnum(BigInteger.class);
            BigInteger zero = tailer.readEnum(BigInteger.class);
            BigInteger one2 = tailer.readEnum(BigInteger.class);
            BigInteger ten2 = tailer.readEnum(BigInteger.class);
            BigInteger zero2 = tailer.readEnum(BigInteger.class);
            excerpt.close();

            assertSame(AccessMode.EXECUTE, e);
            assertSame(AccessMode.READ, r);
            assertSame(AccessMode.WRITE, w);
            assertEquals(BigInteger.ONE, one);
            assertEquals(BigInteger.TEN, ten);
            assertEquals(BigInteger.ZERO, zero);
            assertSame(one, one2);
            assertSame(ten, ten2);
            assertSame(zero, zero2);
        } finally {
            chronicle.close();
            assertClean(basePath);
        }
    }

    enum AccessMode {
        EXECUTE, READ, WRITE
    }
}
