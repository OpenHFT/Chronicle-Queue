/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.bench;

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;

import static net.openhft.chronicle.queue.rollcycles.SparseRollCycles.LARGE_HOURLY_XSPARSE;

/*
Ryzen 9 5950X with Corsair MP600 PRO XT

-Dtime=30 -Dsize=1000000 -Dpath=/data/tmp - DblockSizeMB=524288
Writing 54,626 messages took 30.006 seconds, at a rate of 1,820 per second, with an average latency of 549,290 ns
Reading 54,626 messages took 16.970 seconds, at a rate of 3,219 per second, with an average latency of 310,652 ns

-Dtime=30 -Dsize=100000 -Dpath=/data/tmp - DblockSizeMB=524288
Writing 608,464 messages took 30.001 seconds, at a rate of 20,281 per second, with an average latency of 49,306 ns
Reading 608,464 messages took 21.147 seconds, at a rate of 28,773 per second, with an average latency of 34,754 ns

-Dtime=30 -Dsize=10000 -Dpath=/data/tmp - DblockSizeMB=524288
Writing 4,753,747 messages took 30.001 seconds, at a rate of 158,451 per second, with an average latency of 6,311 ns
Reading 4,753,747 messages took 27.304 seconds, at a rate of 174,101 per second, with an average latency of 5,743 ns

-Dtime=30 -Dsize=1000 -Dpath=/data/tmp - DblockSizeMB=524288
Writing 42,526,957 messages took 30.001 seconds, at a rate of 1,417,508 per second, with an average latency of 705 ns
Reading 42,526,957 messages took 16.796 seconds, at a rate of 2,532,011 per second, with an average latency of 394 ns

-Dtime=30 -Dsize=300 -Dpath=/data/tmp - DblockSizeMB=524288
Writing 107,971,666 messages took 30.001 seconds, at a rate of 3,598,924 per second, with an average latency of 277 ns
Reading 107,971,666 messages took 5.556 seconds, at a rate of 19,433,454 per second, with an average latency of 51 ns

-Dtime=30 -Dsize=100 -Dpath=/data/tmp - DblockSizeMB=524288
Writing 202,468,247 messages took 30.001 seconds, at a rate of 6,748,769 per second, with an average latency of 148 ns
Reading 202,468,247 messages took 7.561 seconds, at a rate of 26,776,278 per second, with an average latency of 37 ns
 */
public class ThroughputPerfMain {
    private static final int TIME = Integer.getInteger("time", 30);
    private static final int SIZE = Integer.getInteger("size", 40);
    private static final String PATH = System.getProperty("path", OS.TMP);
    private static final long blockSizeMB = Long.getLong("blockSizeMB", OS.isSparseFileSupported() ? 512L << 10 : 256L);

    private static BytesStore nbs;

    public static void main(String[] args) {
        String base = PATH + "/delete-" + Time.uniqueId() + ".me";
        long start = System.nanoTime();
        long count = 0;
        nbs = BytesStore.nativeStoreWithFixedCapacity(SIZE);
        AffinityLock lock = AffinityLock.acquireCore();
        try (ChronicleQueue q = ChronicleQueue.singleBuilder(base)
                .rollCycle(LARGE_HOURLY_XSPARSE)
                .blockSize(blockSizeMB << 20)
                .build()) {

            ExcerptAppender appender = q.acquireAppender();
            do {
                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().bytes().write(nbs);
                }
                count++;
            } while (start + TIME * 1e9 > System.nanoTime());
        }

        nbs.releaseLast();
        long mid = System.nanoTime();
        long time1 = mid - start;

        Bytes<?> bytes = Bytes.allocateElasticDirect(SIZE);
        try (ChronicleQueue q = ChronicleQueue.singleBuilder(base)
                .rollCycle(LARGE_HOURLY_XSPARSE)
                .blockSize(blockSizeMB << 20)
                .build()) {
            ExcerptTailer tailer = q.createTailer();
            for (long i = 0; i < count; i++) {
                try (DocumentContext dc = tailer.readingDocument()) {
                    bytes.clear();
                    bytes.write(dc.wire().bytes());
                }
            }
        }
        bytes.releaseLast();
        long end = System.nanoTime();
        long time2 = end - mid;
        lock.close();

        System.out.println("-Dtime=" + TIME +
                " -Dsize=" + SIZE +
                " -Dpath=" + PATH +
                " - DblockSizeMB=" + blockSizeMB);
        System.out.printf("Writing %,d messages took %.3f seconds, at a rate of %,d per second, with an average latency of %,d ns%n",
                count, time1 / 1e9, (long) (1e9 * count / time1), time1/count);
        System.out.printf("Reading %,d messages took %.3f seconds, at a rate of %,d per second, with an average latency of %,d ns%n",
                count, time2 / 1e9, (long) (1e9 * count / time2), time2/count);
        BackgroundResourceReleaser.releasePendingResources();
        System.gc(); // make sure its cleaned up for windows to delete.
        IOTools.deleteDirWithFiles(base, 2);
    }
}
