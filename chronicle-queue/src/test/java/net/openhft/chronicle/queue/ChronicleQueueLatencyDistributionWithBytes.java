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
import net.openhft.chronicle.bytes.IORuntimeException;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Results 27/10/2015 running on a MBP
 * 50/90 99/99.9 99.99/99.999 - worst was 1.5 / 27  104 / 3,740  8,000 / 13,890 - 36,700
 */
public class ChronicleQueueLatencyDistributionWithBytes extends ChronicleQueueTestBase {
    @Test
    public void test() throws Exception{
        Histogram histogram = new Histogram();

        ChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(WireType.BINARY)
                .blockSize(1_000_000_000)
                .build();

        ExcerptAppender appender = queue.createAppender();
        ExcerptTailer tailer = queue.createTailer();

        Thread tailerThread = new Thread(() -> {
            MyReadMarshallable myReadMarshallable = new MyReadMarshallable(histogram);
            AffinityLock lock = null;
            try {
                if(Boolean.getBoolean("enableTailerAffinity")) {
                    lock = Affinity.acquireLock();
                }

                while (true) {
                    try {
                        tailer.readBytes(myReadMarshallable);
                    } catch (IOException e) {
                        e.printStackTrace();
                        break;
                    }
                }
            } finally {
                if(lock != null) {
                    lock.release();
                }
            }
        });

        Thread appenderThread = new Thread(() -> {
            AffinityLock lock = null;
            try {
                if(Boolean.getBoolean("enableAppenderAffinity")) {
                    lock = Affinity.acquireLock();
                }

                TestTrade bt = new TestTrade();
                for (int i = 0; i < 10_000_000; i++) {
                    Jvm.busyWaitMicros(5);
                    bt.setTime(System.nanoTime());
                    appender.writeBytes(bt);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if(lock != null) {
                    lock.release();
                }
            }
        });

        tailerThread.start();

        appenderThread.start();
        appenderThread.join();

        //Pause to allow tailer to catch up (if needed)
        Jvm.pause(500);

        System.out.println(histogram.toMicrosFormat());
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
            if (counter.get() > 1_000_000) {
                histogram.sample(System.nanoTime() - time);
            }
            if (counter.incrementAndGet() % 1_000_000 == 0) {
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
