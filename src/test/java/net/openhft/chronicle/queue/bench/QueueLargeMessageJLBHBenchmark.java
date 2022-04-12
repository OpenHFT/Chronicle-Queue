/*
 * Copyright 2014-2020 chronicle.software
 *
 * http://chronicle.software
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
package net.openhft.chronicle.queue.bench;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import net.openhft.chronicle.jlbh.TeamCityHelper;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;

public class QueueLargeMessageJLBHBenchmark implements JLBHTask {
    private static final int MSG_THROUGHPUT = Integer.getInteger("throughput", 50_000_000);
    private static final int MSG_LENGTH = Integer.getInteger("length", 1_000_000);
    private static final boolean MSG_DIRECT = Jvm.getBoolean("direct");
    static byte[] bytesArr = new byte[MSG_LENGTH];
    static Bytes<?> bytesArr2 = Bytes.allocateDirect(MSG_LENGTH);
    private static int iterations;
    private SingleChronicleQueue sourceQueue;
    private SingleChronicleQueue sinkQueue;
    private ExcerptTailer tailer;
    private ExcerptAppender appender;
    private JLBH jlbh;

    public static void main(String[] args) {
        int throughput = MSG_THROUGHPUT / MSG_LENGTH;
        int warmUp = Math.min(50 * throughput, 12_000);
        iterations = Math.min(20 * throughput, 100_000);

        JLBHOptions lth = new JLBHOptions()
                .warmUpIterations(warmUp)
                .iterations(iterations)
                .throughput(throughput)
                .recordOSJitter(false)
                .skipFirstRun(true)
                .runs(5)
                .jlbhTask(new QueueLargeMessageJLBHBenchmark());
        new JLBH(lth).start();
    }

    @Override
    public void init(JLBH jlbh) {
        IOTools.deleteDirWithFiles("large", 3);

        sourceQueue = single("large").blockSize(1L << 30).build();
        sinkQueue = single("large").blockSize(1L << 30).build();
        appender = sourceQueue.acquireAppender();
        tailer = sinkQueue.createTailer().disableThreadSafetyCheck(true);
        this.jlbh = jlbh;
    }

    @Override
    public void run(long startTimeNS) {

        if (MSG_DIRECT)
            bytesArr2.readLimit(MSG_LENGTH);
        try (DocumentContext dc = appender.writingDocument()) {
            Bytes<?> bytes = dc.wire().bytes();
            bytes.writeLong(startTimeNS);
            bytes.writeInt(bytes.length());
            if (MSG_DIRECT)
                bytes.write(bytesArr2);
            else
                bytes.write(bytesArr);
        }

        try (DocumentContext dc = tailer.readingDocument()) {
            if (dc.wire() != null) {
                Bytes<?> bytes = dc.wire().bytes();
                long start = bytes.readLong();
                int length = bytes.readInt();
                assert length == MSG_LENGTH;
                if (MSG_DIRECT)
                    bytes.read(bytesArr2.clear(), length);
                else
                    bytes.read(bytesArr);
                jlbh.sample(System.nanoTime() - start);
            }
        }
    }

    @Override
    public void complete() {
        sinkQueue.close();
        sourceQueue.close();
        TeamCityHelper.teamCityStatsLastRun(getClass().getSimpleName(), jlbh, iterations, System.out);
    }
}
