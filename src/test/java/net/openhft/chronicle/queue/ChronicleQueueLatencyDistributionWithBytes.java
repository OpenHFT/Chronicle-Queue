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

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.NativeBytes;
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.threads.BusyPauser;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.chronicle.wire.WireType;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Result on 18/1/2016 running on i7-3970X Ubuntu 10.04 with affinity writing to tmpfs write: 50/90
 * 99/99.9 99.99/99.999 - worst was 1.7 / 1.8  1.9 / 2.4  184 / 1,800 - 2,160 write-read: 50/90
 * 99/99.9 99.99/99.999 - worst was 4.7 / 7.3  418 / 5,900  12,320 / 14,940 - 14,940 <p> Result on
 * 18/1/2016 running on i7-3970X Ubuntu 10.04 with affinity writing to ext4 on Samsung 840 SSD
 * write: 50/90 99/99.9 99.99/99.999 - worst was 1.7 / 1.8  1.8 / 2.4  1,610 / 4,330 - 4,590
 * write-read: 50/90 99/99.9 99.99/99.999 - worst was 4.7 / 7.6  20,450 / 155,190  188,740 / 188,740
 * - 188,740 <p> Result on an E5-2650 v2 @ 2.60GHz, Ubuntu 10.04 writing to ext4. write: 50/90
 * 99/99.9 99.99/99.999 99.9999/worst was 0.21 / 0.98  1.3 / 5.2  14 / 42  606 / 868 write-read:
 * 50/90 99/99.9 99.99/99.999 99.9999/worst was 0.62 / 1.6  13 / 6,160  66,060 / 81,790  85,980 /
 * 85,980 <p> Result on an i7-4790 @ 3.6 GHz Centos 7 to tmpfs write: 50/90 99/99.9 99.99/99.999
 * 99.9999/worst was 0.15 / 0.17  0.20 / 0.28  9.5 / 10  11 / 1,410 write-read: 50/90 99/99.9
 * 99.99/99.999 99.9999/worst was 0.56 / 0.66  4.0 / 10  34,600 / 40,890  40,890 / 40,890 <p> write:
 * 50/90 99/99.9 99.99/99.999 99.9999/worst was 0.15 / 0.16  0.23 / 0.56  9.5 / 10  12 / 68
 * write-read: 50/90 99/99.9 99.99/99.999 99.9999/worst was 0.53 / 0.69  7.0 / 23  34,600 / 34,600
 * 34,600 / 34,600
 */
public class ChronicleQueueLatencyDistributionWithBytes extends ChronicleQueueTestBase {

    public static final int BYTES_LENGTH = 256;
    public static final int BLOCK_SIZE = 256 << 20;
    public static final int BUFFER_CAPACITY = 256 << 10;
    private static final long INTERVAL_US = 10;

    @Ignore("long running")
    @Test
    public void test() throws IOException, InterruptedException {
        Histogram histogram = new Histogram();
        Histogram writeHistogram = new Histogram();

        String path = "deleteme" + System.nanoTime() + ".q";
//        String path = getTmpDir() + "/deleteme.q";

        new File(path).deleteOnExit();
        ChronicleQueue rqueue = new SingleChronicleQueueBuilder(path)
                .wireType(WireType.FIELDLESS_BINARY)
                .blockSize(BLOCK_SIZE)
                .build();

        EventGroup eventLoop = new EventGroup(true, Throwable::printStackTrace, BusyPauser.INSTANCE, true);
        ChronicleQueue wqueue = new SingleChronicleQueueBuilder(path)
                .wireType(WireType.FIELDLESS_BINARY)
                .blockSize(BLOCK_SIZE)
                .bufferCapacity(BUFFER_CAPACITY)
                .buffered(true)
                .eventLoop(eventLoop)
                .build();

        ExcerptAppender appender = wqueue.createAppender();
        ExcerptTailer tailer = rqueue.createTailer();

        Thread tailerThread = new Thread(() -> {
            AffinityLock rlock = AffinityLock.acquireLock();
            Bytes bytes = NativeBytes.nativeBytes(BYTES_LENGTH).unchecked(true);
            try {

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
                if (rlock != null) {
                    rlock.release();
                }
            }
        }, "tailer thread");

        Thread appenderThread = new Thread(() -> {
            AffinityLock wlock = AffinityLock.acquireLock();
            try {
                Bytes bytes = Bytes.allocateDirect(BYTES_LENGTH).unchecked(true);

                long next = System.nanoTime() + INTERVAL_US * 1000;
                for (int i = 0; i < 20_000_000; i++) {
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
                if (wlock != null) {
                    wlock.release();
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
        eventLoop.close();
    }
}
