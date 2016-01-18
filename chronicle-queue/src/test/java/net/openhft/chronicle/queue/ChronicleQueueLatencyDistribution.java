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
import net.openhft.chronicle.bytes.IORuntimeException;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Results 27/10/2015 running on a MBP 50/90 99/99.9 99.99/99.999 - worst was 1.5 / 27  104 / 3,740
 * 8,000 / 13,890 - 36,700
 */
public class ChronicleQueueLatencyDistribution extends ChronicleQueueTestBase {
    @Ignore("long running")
    @Test
    public void test() throws Exception {
        Histogram histogram = new Histogram();

        ChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(WireType.FIELDLESS_BINARY)
                .blockSize(128 << 20)
                .build();

        ExcerptAppender appender = queue.createAppender();
        ExcerptTailer tailer = queue.createTailer();

        Thread tailerThread = new Thread(() -> {
            MyReadMarshallable myReadMarshallable = new MyReadMarshallable(histogram);
            AffinityLock lock = null;
            try {
                if (Boolean.getBoolean("enableTailerAffinity")) {
                    lock = Affinity.acquireLock();
                }

                while (true) {
                    try {
                        tailer.readDocument(myReadMarshallable);
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
        });

        Thread appenderThread = new Thread(() -> {
            AffinityLock lock = null;
            try {
                if (Boolean.getBoolean("enableAppenderAffinity")) {
                    lock = Affinity.acquireLock();
                }

                TestTrade bt = new TestTrade();
                MyWriteMarshallable myWriteMarshallable = new MyWriteMarshallable(bt);
                for (int i = 0; i < 1_000_000; i++) {
                    Jvm.busyWaitMicros(20);
                    bt.setTime(System.nanoTime());
                    appender.writeDocument(myWriteMarshallable);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (lock != null) {
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

    static class MyWriteMarshallable implements WriteMarshallable {
        private TestTrade bt;

        public MyWriteMarshallable(TestTrade bt) {

            this.bt = bt;
        }

        @Override
        public void writeMarshallable(WireOut w) {
            w.write(() -> "TestTrade")
                    .marshallable(bt);
        }
    }

    static class MyReadMarshallable implements ReadMarshallable {
        StringBuilder messageType = new StringBuilder();
        AtomicInteger counter = new AtomicInteger(0);
        private Histogram histogram;
        TestTrade testTrade = new TestTrade();

        public MyReadMarshallable(Histogram histogram) {
            this.histogram = histogram;
        }

        @Override
        public void readMarshallable(WireIn wireIn) throws IORuntimeException {
            ValueIn vi = wireIn.readEventName(messageType);
            vi.marshallable(testTrade);

            long time = testTrade.getTime();
            if (counter.get() > 100_000) {
                histogram.sample(System.nanoTime() - time);
            }
            if (counter.incrementAndGet() % 100_000 == 0) {
                System.out.println(counter.get());
            }
        }
    }

    static class TestTrade implements Marshallable {
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
        public void readMarshallable(@NotNull WireIn wire) throws IORuntimeException {
            wire.read(() -> "price").int32(this, (o, b) -> o.price = b)
                    .read(() -> "id").text(this, (o, b) -> o.id = b)
                    .read(() -> "time").int64(this, (o, b) -> o.time = b);
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            wire.write(() -> "price").int32(price)
                    .write(() -> "id").text(id)
                    .write(() -> "time").int64(time);
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
