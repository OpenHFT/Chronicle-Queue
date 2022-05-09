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
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.NanoSampler;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;

/*
Chronicle Queue performance is compared when using sparse files and chunking( with standard and large blockSize).
 */
public class QueueSparseFilesJLBHBenchmark implements JLBHTask {
    private static int iterations;
    private SingleChronicleQueue sourceQueue;
    private SingleChronicleQueue sinkQueue;
    private ExcerptTailer tailer;
    private ExcerptAppender appender;
    private JLBH jlbh;
    private NanoSampler writeProbe;
    private NanoSampler readProbe;
    private static int round = 1;
    private int count = 0;

    public static void main(String[] args) {
        int throughput = 100_000;
        int warmUp = 200_000;
        iterations = 200_000;

        JLBHOptions lth = new JLBHOptions()
                .warmUpIterations(warmUp)
                .iterations(iterations)
                .throughput(throughput)
                .recordOSJitter(false)
                .skipFirstRun(true)
                .runs(5)
                .jlbhTask(new QueueSparseFilesJLBHBenchmark());
        new JLBH(lth).start();
    }

    @Override
    public void init(JLBH jlbh) {
        IOTools.deleteDirWithFiles("benchmark", 5);

        if (round == 1) {
            // Uses sparse file for memory mapping
            sourceQueue = single("benchmark/sparseFile").useSparseFiles(true).sparseCapacity(512L << 32).build();
            sinkQueue = single("benchmark/sparseFile").useSparseFiles(true).sparseCapacity(512L << 32).build();
        } else if (round == 2) {
            // UsesConfigures chunking for memory mapping using a large memory block
            sourceQueue = single("benchmark/chunking-largeBlockSize").blockSize(64L << 30).build();
            sinkQueue = single("benchmark/chunking-largeBlockSize").blockSize(64L << 30).build();
        } else {
            // Configures chunking for memory mapping using a small memory block
            sourceQueue = single("benchmark/chunking-standardBlockSize").blockSize(256L << 20).build();
            sinkQueue = single("benchmark/chunking-standardBlockSize").blockSize(256L << 20).build();
        }
        appender = sourceQueue.acquireAppender();
        tailer = sinkQueue.createTailer().disableThreadSafetyCheck(true);
        this.jlbh = jlbh;
        writeProbe = round == 1 ? jlbh.addProbe("write (sparse file)") : round == 2 ? jlbh.addProbe("write (chunking-largeBlockSize)") : jlbh.addProbe("write (chunking-standardBlockSize)");
        readProbe = round == 1 ? jlbh.addProbe("read (sparse file)") : round == 2 ? jlbh.addProbe("read (chunking-largeBlockSize)") : jlbh.addProbe("read (chunking-standardBlockSize)");
    }

    @Override
    public void run(long startTimeNS) {
        try (DocumentContext dc = appender.writingDocument()) {
            Bytes bytes = dc.wire().bytes();
            bytes.writeSkip(1L << 20);
            bytes.writeInt(count);
            writeProbe.sampleNanos(System.nanoTime() - startTimeNS);
        }

        long beforeReadNS = System.nanoTime();
        try (DocumentContext dc = tailer.readingDocument()) {
            if (dc.wire() != null) {
                Bytes bytes = dc.wire().bytes();
                bytes.readSkip(1L << 20);
                bytes.readInt();
                long afterReadNS = System.nanoTime();
                jlbh.sample(afterReadNS - startTimeNS);
                readProbe.sampleNanos(afterReadNS - beforeReadNS);
            }
        }
        count++;
    }

    @Override
    public void complete() {
        sinkQueue.close();
        sourceQueue.close();
        if (round != 3) {
            round++;
            main(null);
        }
    }
}
