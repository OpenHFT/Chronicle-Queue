/*
 * Copyright 2014-2020 chronicle.software
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
package net.openhft.chronicle.queue.bench.multiprocess;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.NanoSampler;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.bench.CLIUtils;
import net.openhft.chronicle.queue.bench.JLBHResultSerializer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.FileNotFoundException;
import java.io.IOException;

import static net.openhft.chronicle.queue.bench.CLIUtils.addOption;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;
import static net.openhft.chronicle.queue.rollcycles.LargeRollCycles.LARGE_DAILY;

public class QueueTwoServicesJLBHBenchmark implements JLBHTask {
    public static final int DEFAULT_ITERATIONS = 100_000;
    public static final String THE_PROBE = "TheProbe";
    private SingleChronicleQueue queue;
    private ExcerptTailer tailer;
    private JLBH jlbh;
    private static Datum datum ;
    private static NanoSampler theProbe;

    static {
        System.setProperty("disable.thread.safety", "true");
        System.setProperty("jvm.resource.tracing", "false");
        System.setProperty("check.thread.safety", "false");
    }


    public static void main(String[] args) throws FileNotFoundException {
        Options options = CLIUtils.createOptions();
        addOption(options, "p", "payload", true, "Payload Size (approximate)", false);

        CommandLine commandLine = CLIUtils.parseCommandLine(args, options);

        datum = new Datum(CLIUtils.getIntOption(commandLine, 'p', 128));

        // disable as otherwise single GC event skews results heavily
        int iterations = CLIUtils.getIntOption(commandLine, 'i', DEFAULT_ITERATIONS);
        JLBHOptions lth = new JLBHOptions()
                .warmUpIterations(50_000)
                .iterations(iterations)
                .throughput(CLIUtils.getIntOption(commandLine, 't', 10_000))
                .recordOSJitter(commandLine.hasOption('j')).accountForCoordinatedOmission(false)
                .skipFirstRun(true)
                .runs(CLIUtils.getIntOption(commandLine, 'r', 3))
                .jlbhTask(new QueueTwoServicesJLBHBenchmark());
        new JLBH(lth, System.out, jlbhResult -> {
            if (commandLine.hasOption('f')) {
                try {
                    JLBHResultSerializer.runResultToCSV(jlbhResult, "result.csv",
                            QueueTwoServicesJLBHBenchmark.THE_PROBE);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();

    }

    @Override
    public void init(JLBH jlbh) {
        IOTools.deleteDirWithFiles("replica", 10);

        this.jlbh = jlbh;
        queue = single("replica").rollCycle(LARGE_DAILY).doubleBuffer(false).build();
        theProbe = jlbh.addProbe(THE_PROBE);
        tailer = queue.createTailer();
        tailer.singleThreadedCheckDisabled(true);
        tailer.toStart();
    }

    long written = 0;

    @Override
    public void run(long startTimeNS) {
        while (true) {
            try (DocumentContext dc = tailer.readingDocument()) {
                if (dc.wire() == null)
                    continue;
                if (dc.wire().read("datum").marshallable(datum)) {
                    long ts = datum.ts;
                    long nanoTime = System.nanoTime();

                    long durationNs = nanoTime - ts;
                    if (written < 10 || written % 10000 == 0) {
                        System.out.println("nanoTime " + nanoTime + " ts = " + ts + " durationNs = " + durationNs);
                    }
                    theProbe.sampleNanos(durationNs);
                    break;
                }

            }
        }

        jlbh.sampleNanos(System.nanoTime() - startTimeNS);
        written++;
        if (written % 10_000 == 0)
            System.err.println("Written: " + written);
    }

    @Override
    public void complete() {
//        stopped = true;
        queue.close();
    }


}
