/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue;

import net.openhft.affinity.Affinity;
import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Results 27/10/2015 running on a MBP
 * 50/90 99/99.9 99.99/99.999 - worst was 1.5 / 27  104 / 3,740  8,000 / 13,890 - 36,700
 *
 * Results 14/03/2016 running on E5-2650v2
 * 50/90 99/99.9 99.99 - worst was 0.88 / 1.4  10.0 / 19  72 - 483
 *
 * Results 23/03/2016 running on E5-2643 Debian Kernel 4.2
 * 50/90 99/99.9 99.99 - worst was 0.56 / 0.82  5.0 / 12  40 - 258
 *
 * Results 23/03/2016 running on Linux VM (i7-4800MQ) Debian Kernel 4.2
 * 50/90 99/99.9 99.99 - worst was 0.50 / 1.6  21 / 84  573 - 1,410
 *
 * Results 23/03/2016 running on E3-1505Mv5 Debian Kernel 4.5
 * 50/90 99/99.9 99.99 - worst was 0.33 / 0.36  1.6 / 3.0  18 - 160
 */
public class ChronicleQueueLatencyDistribution extends ChronicleQueueTestBase {
    @Ignore("long running")
    @Test
    public void test() throws IOException, InterruptedException {
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
                    } catch (Exception e) {
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
            } catch (Exception e) {
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
        private final TestTrade bt;

        public MyWriteMarshallable(TestTrade bt) {

            this.bt = bt;
        }

        @Override
        public void writeMarshallable(@NotNull WireOut w) {
            w.write(() -> "TestTrade")
                    .marshallable(bt);
        }
    }

    static class MyReadMarshallable implements ReadMarshallable {
        final StringBuilder messageType = new StringBuilder();
        final AtomicInteger counter = new AtomicInteger(0);
        final TestTrade testTrade = new TestTrade();
        private final Histogram histogram;

        public MyReadMarshallable(Histogram histogram) {
            this.histogram = histogram;
        }

        @Override
        public void readMarshallable(@NotNull WireIn wireIn) throws IORuntimeException {
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
