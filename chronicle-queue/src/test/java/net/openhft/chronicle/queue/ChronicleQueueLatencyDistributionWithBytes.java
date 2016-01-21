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
import net.openhft.chronicle.bytes.NativeBytes;
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Result on 18/1/2016 running on i7-3970X Ubuntu 10.04 with affinity writing to tmpfs
 * write: 50/90 99/99.9 99.99/99.999 - worst was 1.7 / 1.8  1.9 / 2.4  184 / 1,800 - 2,160
 * write-read: 50/90 99/99.9 99.99/99.999 - worst was 4.7 / 7.3  418 / 5,900  12,320 / 14,940 - 14,940
 * <p>
 * Result on 18/1/2016 running on i7-3970X Ubuntu 10.04 with affinity writing to ext4 on Samsung 840 SSD
 * write: 50/90 99/99.9 99.99/99.999 - worst was 1.7 / 1.8  1.8 / 2.4  1,610 / 4,330 - 4,590
 * write-read: 50/90 99/99.9 99.99/99.999 - worst was 4.7 / 7.6  20,450 / 155,190  188,740 / 188,740 - 188,740
 * <p>
 * write: 50/90 99/99.9 99.99/99.999 - worst was 1.8 / 2.6  5.2 / 13  121 / 319 - 672
 * write-read: 50/90 99/99.9 99.99/99.999 - worst was 2.2 / 3.8  5.8 / 13  258 / 516 - 1,210
 * <p>
 * write: 50/90 99/99.9 99.99/99.999 - worst was 0.49 / 1.1  3.6 / 80  8,650 / 20,450 - 22,540
 * write-read: 50/90 99/99.9 99.99/99.999 99.9999/worst was 0.53 / 1.1  3.8 / 6,160  17,300 / 21,500  23,590 / 23,590
 * <p>
 * write: 50/90 99/99.9 99.99/99.999 - worst was 1.5 / 1.5  1.6 / 2.2  65 / 1,740 - 3,600
 * write-read: 50/90 99/99.9 99.99/99.999 99.9999/worst was 3.9 / 6.5  225 / 15,990  106,950 / 115,340  115,340 / 115,340
 */
public class ChronicleQueueLatencyDistributionWithBytes extends ChronicleQueueTestBase {

    public static final int BYTES_LENGTH = 128;
    public static final int BLOCK_SIZE = 16 << 20;
    private static final long INTERVAL_US = 20;

    //  @Ignore("long running")
    @Test
    public void test() throws IOException, InterruptedException {
        Histogram histogram = new Histogram();
        Histogram writeHistogram = new Histogram();

        String path = "target/deleteme" + System.nanoTime() + ".q"; /*getTmpDir()*/
//        String path = getTmpDir() + "/deleteme.q";
        new File(path).deleteOnExit();
        ChronicleQueue rqueue = new SingleChronicleQueueBuilder(path)
                .wireType(WireType.FIELDLESS_BINARY)
                .blockSize(BLOCK_SIZE)
                .bufferCapacity(64 << 10)
                .build();

        ChronicleQueue wqueue = new SingleChronicleQueueBuilder(path)
                .wireType(WireType.FIELDLESS_BINARY)
                .blockSize(BLOCK_SIZE)
                .bufferCapacity(64 << 10)
                .buffered(true)
                .build();

        ExcerptAppender appender = wqueue.createAppender();
        ExcerptTailer tailer = rqueue.createTailer();

        Thread tailerThread = new Thread(() -> {

            Bytes bytes = NativeBytes.nativeBytes(BYTES_LENGTH).unchecked(true);
            //   Bytes bytes = Bytes.allocateDirect(BYTES_LENGTH).unchecked(true);
            AffinityLock lock = null;
            try {
                if (Boolean.getBoolean("enableTailerAffinity")) {
                    lock = Affinity.acquireLock();
                }

                while (true) {
                    try {
                        bytes.clear();
                        if (tailer.readBytes(bytes)) {
                            histogram.sample(System.nanoTime() - bytes.readLong(0));
                        } else {
                            tailer.prefetch();
                        }
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
                Bytes bytes = Bytes.allocateDirect(BYTES_LENGTH).unchecked(true);

                long next = System.nanoTime() + INTERVAL_US * 1000;
                for (int i = 0; i < 1_000_000; i++) {
                    while (System.nanoTime() < next)
                        /* busy wait*/ ;
                    long start = next;
                    bytes.readPosition(0);
                    bytes.readLimit(BYTES_LENGTH);
                    bytes.writeLong(0L, start);
                    long start2 = System.nanoTime();

                    appender.writeBytes(bytes);
                    if (i > 200000)
                        writeHistogram.sample(System.nanoTime() - start2);
                    next += INTERVAL_US * 1000;
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
        Thread.sleep(100);

        appenderThread.start();
        appenderThread.join();
//        prefetcher.interrupt();
//        prefetcher.join();

        //Pause to allow tailer to catch up (if needed)
        tailerThread.join(100);

        System.out.println("write: " + writeHistogram.toMicrosFormat());
        System.out.println("write-read: " + histogram.toMicrosFormat());
//        rqueue.close();
//        wqueue.close();
    }
}
