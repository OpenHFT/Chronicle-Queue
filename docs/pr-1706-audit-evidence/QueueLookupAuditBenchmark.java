/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
import com.sun.management.ThreadMXBean;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.rollcycles.LargeRollCycles;
import net.openhft.chronicle.queue.rollcycles.SparseRollCycles;
import net.openhft.chronicle.wire.DocumentContext;

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

/** Diagnostic batches, not latency acceptance thresholds; run separate JVMs for comparisons. */
public final class QueueLookupAuditBenchmark {
    private static volatile long sink;

    public static void main(String[] args) throws Exception {
        String revision = System.getProperty("revision");
        RollCycle cycle = args[0].equals("FAST_DAILY") ? RollCycles.FAST_DAILY :
                args[0].equals("HUGE_DAILY") ? LargeRollCycles.HUGE_DAILY : SparseRollCycles.HUGE_DAILY_XSPARSE;
        Path directory = Files.createTempDirectory("queue-lookup-audit-");
        ThreadMXBean allocations = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        allocations.setThreadAllocatedMemoryEnabled(true);
        long thread = Thread.currentThread().getId();
        Bytes<?> payload = Bytes.wrapForRead(new byte[8]);
        System.out.println("java=" + System.getProperty("java.runtime.version") + " os=" + System.getProperty("os.arch")
                + " options=" + ManagementFactory.getRuntimeMXBean().getInputArguments());
        System.out.println("revision,cycle,operation,round,population,iterations,ns_per_op,bytes_per_op");
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .rollCycle(cycle).timeProvider(() -> 0L).build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer()) {
            long population = 0;
            if (cycle != RollCycles.FAST_DAILY) {
                appender.writeBytes(b -> b.writeSkip(cycle == LargeRollCycles.HUGE_DAILY ? 16 << 20 : 1 << 16));
                population++;
            }
            for (int i = 0; i < 10_000; i++) {
                write(appender, population++);
            }
            for (String operation : new String[]{"document", "bytes", "toEnd", "lastIndex", "count", "moveNearTail"}) {
                int iterations = operation.equals("document") || operation.equals("bytes") ? 20_000 : 5_000;
                for (int round = 0; round <= 4; round++) {
                    int count = round == 0 ? 20_000 : iterations;
                    long allocatedBefore = allocations.getThreadAllocatedBytes(thread);
                    long start = System.nanoTime();
                    for (int i = 0; i < count; i++) {
                        switch (operation) {
                            case "document": write(appender, population++); break;
                            case "bytes": appender.writeBytes(payload); population++; break;
                            case "toEnd": tailer.toEnd(); sink = tailer.index(); break;
                            case "lastIndex": sink = queue.lastIndex(); break;
                            case "count": sink = tailer.excerptsInCycle(0); break;
                            case "moveNearTail":
                                long sequence = population - 1 - ((i * 17) & 63);
                                if (!tailer.moveToIndex(cycle.toIndex(0, sequence)))
                                    throw new AssertionError("missing sequence " + sequence);
                                sink = tailer.index();
                                break;
                            default: throw new AssertionError(operation);
                        }
                    }
                    long elapsed = System.nanoTime() - start;
                    long allocated = allocations.getThreadAllocatedBytes(thread) - allocatedBefore;
                    if (round != 0)
                        System.out.printf(Locale.ROOT, "%s,%s,%s,%d,%d,%d,%.2f,%.2f%n", revision, cycle,
                                operation, round, population, count, (double) elapsed / count, (double) allocated / count);
                    // Correctness assertions are outside both measured regions.
                    if (operation.equals("toEnd") && cycle.toSequenceNumber(sink) != population)
                        throw new AssertionError("toEnd " + sink + " population " + population);
                    if (operation.equals("lastIndex") && cycle.toSequenceNumber(sink) != population - 1)
                        throw new AssertionError("lastIndex " + sink + " population " + population);
                    if (operation.equals("count") && sink != population)
                        throw new AssertionError("count " + sink + " population " + population);
                }
            }
            if (tailer.excerptsInCycle(0) != population)
                throw new AssertionError("final population " + population);
        } finally {
            payload.releaseLast();
            IOTools.deleteDirWithFiles(directory.toFile());
        }
    }

    private static void write(ExcerptAppender appender, long value) {
        try (DocumentContext dc = appender.writingDocument()) {
            dc.wire().bytes().writeLong(value);
        }
    }
}
