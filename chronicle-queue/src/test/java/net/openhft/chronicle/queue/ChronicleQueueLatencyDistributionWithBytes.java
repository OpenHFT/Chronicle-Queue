/*
 *
 *    Copyright (C) 2015  higherfrequencytrading.com
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU Lesser General Public License as published by
 *    the Free Software Foundation, either version 3 of the License.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Lesser General Public License for more details.
 *
 *    You should have received a copy of the GNU Lesser General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.chronicle.queue;

import net.openhft.affinity.Affinity;
import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Result on 18/1/2016 running on i7-3970X Ubuntu 10.04 with affinity writing to tmpfs write: 50/90
 * 99/99.9 99.99/99.999 - worst was 1.6 / 2.8  4.7 / 14  31 / 1,080 - 27,790 write-read: 50/90
 * 99/99.9 99.99/99.999 - worst was 2.0 / 3.1  4.7 / 14  967 / 9,180 - 18,350 <p> Result on
 * 18/1/2016 running on i7-3970X Ubuntu 10.04 with affinity writing to ext4 on Samsung 840 SSD
 * write: 50/90 99/99.9 99.99/99.999 - worst was 1.6 / 2.2  4.7 / 28  36 / 160 - 29,880 write-read:
 * 50/90 99/99.9 99.99/99.999 - worst was 2.1 / 2.5  5.8 / 113  160 / 1,670 - 20,450 <p> Results
 * 27/10/2015 running on a MBP 50/90 99/99.9 99.99/99.999 - worst was 1.5 / 27  104 / 3,740 8,000 /
 * 13,890 - 36,700
 */
public class ChronicleQueueLatencyDistributionWithBytes extends ChronicleQueueTestBase {

    public static final int STRING_LENGTH = 192;
    private static final long INTERVAL_US = 20;
    public static final int BLOCK_SIZE = 16 << 20;

    //   @Ignore("long running")
    @Test
    public void test() throws Exception {
        Histogram histogram = new Histogram();
        Histogram writeHistogram = new Histogram();

        String path = "target/deleteme" + System.nanoTime() + ".q"; /*getTmpDir()*/
        new File(path).deleteOnExit();
        ChronicleQueue rqueue = new SingleChronicleQueueBuilder(path)
                .wireType(WireType.FIELDLESS_BINARY)
                .blockSize(BLOCK_SIZE)
                .build();

        ChronicleQueue wqueue = new SingleChronicleQueueBuilder(path)
                .wireType(WireType.FIELDLESS_BINARY)
                .blockSize(BLOCK_SIZE)
                .buffered(true)
                .build();

        ExcerptAppender appender = wqueue.createAppender();
        ExcerptTailer tailer = rqueue.createTailer();

        Thread tailerThread = new Thread(() -> {
            MyReadMarshallable myReadMarshallable = new MyReadMarshallable(histogram);
            AffinityLock lock = null;
            try {
                if (Boolean.getBoolean("enableTailerAffinity")) {
                    lock = Affinity.acquireLock();
                }

                while (true) {
                    try {
//                        tailer.readBytes(myReadMarshallable);
                        if (!tailer.readBytes(myReadMarshallable))
                            tailer.prefetch();
                    } catch (IOException e) {
                        e.printStackTrace();
                        break;
                    }
                }
            } finally {
                if (lock != null) {
                    lock.release();
                }
            }
        }, "tailer thread");

        Thread appenderThread = new Thread(() -> {
            AffinityLock lock = null;
            try {
                if (Boolean.getBoolean("enableAppenderAffinity")) {
                    lock = Affinity.acquireLock();
                }

                char[] value = new char[STRING_LENGTH];
                Arrays.fill(value, 'X');
                String id = new String(value);
                TestTrade bt = new TestTrade();
                bt.setId(id);
                long next = System.nanoTime() + INTERVAL_US * 1000;
                for (int i = 0; i < 2_000_000; i++) {
                    while (System.nanoTime() < next)
                        /* busy wait*/ ;
                    long start = next;
                    bt.setTime(start);
                    appender.writeBytes(bt);
                    writeHistogram.sample(System.nanoTime() - start);
                    next += INTERVAL_US * 1000;
                    if (next > System.nanoTime())
                        appender.prefetch();
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (lock != null) {
                    lock.release();
                }
            }
        }, "appender thread");

        tailerThread.start();

        appenderThread.start();
        appenderThread.join();

        //Pause to allow tailer to catch up (if needed)
        Jvm.pause(500);

        System.out.println("write: " + writeHistogram.toMicrosFormat());
        System.out.println("write-read: " + histogram.toMicrosFormat());
//        rqueue.close();
//        wqueue.close();
    }

    static class MyReadMarshallable implements ReadBytesMarshallable {
        private AtomicInteger counter = new AtomicInteger(0);
        private TestTrade testTrade = new TestTrade();
        private Histogram histogram;

        public MyReadMarshallable(Histogram histogram) {
            this.histogram = histogram;
        }

        @Override
        public void readMarshallable(Bytes in) throws IORuntimeException {
            testTrade.readMarshallable(in);

            long time = testTrade.getTime();
            if (counter.get() > 200_000) {
                histogram.sample(System.nanoTime() - time);
            }
            if (counter.incrementAndGet() % 200_000 == 0) {
                System.out.println(counter.get());
            }
        }
    }

    static class TestTrade implements BytesMarshallable {
        private int price;
        private String id;
        private long time;

        public long getTime() {
            return time;
        }

        public void setTime(long time) {
            this.time = time;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public int getPrice() {
            return price;
        }

        public void setPrice(int price) {
            this.price = price;
        }

        @Override
        public void readMarshallable(@NotNull Bytes in) {
            this.price = in.readInt();
            this.id = in.readUtf8();
            this.time = in.readLong();
        }

        @Override
        public void writeMarshallable(@NotNull Bytes out) {
            out.writeInt(this.price);
            out.writeUtf8(this.id);
            out.writeLong(this.time);
        }

        @Override
        public String toString() {
            return "TestTrade{" +
                    "price=" + price +
                    ", id='" + id + '\'' +
                    ", time=" + time +
                    '}';
        }
    }
}
