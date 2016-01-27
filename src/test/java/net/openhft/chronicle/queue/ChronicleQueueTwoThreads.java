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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.NativeBytes;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

public class ChronicleQueueTwoThreads extends ChronicleQueueTestBase {

    private static final int BYTES_LENGTH = 256;
    private static final int BLOCK_SIZE = 256 << 20;
    private static final long INTERVAL_US = 10;

    @Test(timeout = 5000)
    public void testUnbuffered() throws IOException, InterruptedException {
        String path = getTmpDir() + "/deleteme.q";

        new File(path).deleteOnExit();
        ChronicleQueue rqueue = new SingleChronicleQueueBuilder(path)
                .wireType(WireType.FIELDLESS_BINARY)
                .blockSize(BLOCK_SIZE)
                .build();

        ChronicleQueue wqueue = new SingleChronicleQueueBuilder(path)
                .wireType(WireType.FIELDLESS_BINARY)
                .blockSize(BLOCK_SIZE)
                .build();

        ExcerptAppender appender = wqueue.createAppender();
        ExcerptTailer tailer = rqueue.createTailer();

        AtomicLong counter = new AtomicLong();
        Thread tailerThread = new Thread(() -> {

            Bytes bytes = NativeBytes.nativeBytes(BYTES_LENGTH).unchecked(true);
            long count = 0;
            try {
                while (true) {
                    bytes.clear();
                    if (tailer.readBytes(bytes)) {
                        counter.incrementAndGet();
                    } else {
                        tailer.prefetch();
                    }
                }
            } catch (Throwable e) {
                e.printStackTrace();
            } finally {
                System.out.printf("Read %,d messages", count);
            }
        }, "tailer thread");

        long runs = 20_000;

        Thread appenderThread = new Thread(() -> {

            try {
                Bytes bytes = Bytes.allocateDirect(BYTES_LENGTH).unchecked(true);

                long next = System.nanoTime() + INTERVAL_US * 1000;
                for (int i = 0; i < runs; i++) {
                    while (System.nanoTime() < next)
                        /* busy wait*/ ;
                    long start = next;
                    bytes.readPosition(0);
                    bytes.readLimit(BYTES_LENGTH);
                    bytes.writeLong(0L, start);

                    appender.writeBytes(bytes);
                    next += INTERVAL_US * 1000;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "appender thread");

        tailerThread.start();
        Thread.sleep(100);

        appenderThread.start();
        appenderThread.join();

        //Pause to allow tailer to catch up (if needed)
        Thread.sleep(100);
        tailerThread.interrupt();
        tailerThread.join(1000);

        while (counter.get() < 20000) {
            Thread.yield();
        }

        assertEquals(runs, counter.get());

    }


}
