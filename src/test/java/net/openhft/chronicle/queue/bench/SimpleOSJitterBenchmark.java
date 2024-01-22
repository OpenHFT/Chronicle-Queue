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

import net.openhft.chronicle.core.util.NanoSampler;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import net.openhft.chronicle.jlbh.util.JLBHResultSerializer;
import net.openhft.chronicle.queue.bench.interprocess.ProducerService;
import net.openhft.chronicle.queue.bench.util.CLIUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class SimpleOSJitterBenchmark implements JLBHTask {

    public static final int DEFAULT_ITERATIONS = 60;
    public static final int DEFAULT_THROUGHPUT = 1;
    private JLBH jlbh;
    private NanoSampler theProbe;

    public static void main(String[] args) {
        Options options = CLIUtils.createOptions();
        CommandLine commandLine = CLIUtils.parseCommandLine(ProducerService.class.getSimpleName(), args, options);

        //Create the JLBH options you require for the benchmark
        int iterations = CLIUtils.getIntOption(commandLine, 'i', DEFAULT_ITERATIONS);
        JLBHOptions lth = new JLBHOptions()
                .warmUpIterations(iterations)
                .iterations(iterations)
                .throughput(CLIUtils.getIntOption(commandLine, 't', DEFAULT_THROUGHPUT))
                .accountForCoordinatedOmission(commandLine.hasOption('c'))
                .recordOSJitter(true)
                .runs(CLIUtils.getIntOption(commandLine, 'r', 3))
                .jlbhTask(new SimpleOSJitterBenchmark());
        new JLBH(lth,System.out, jlbhResult -> {
            try {
                JLBHResultSerializer.runResultToCSV(jlbhResult);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).start();
    }

    @Override
    public void init(JLBH jlbh) {
        this.jlbh = jlbh;
        theProbe = jlbh.addProbe(JLBHResultSerializer.THE_PROBE);
    }

    @Override
    public void run(long startTimeNS) {
//        long start = System.nanoTime();          // (1)
        long start = startTimeNS;                       // (2)
//        System.out.println(new Date()+" sdfasdf");
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
        final long delta = System.nanoTime() - start;
        jlbh.sample(delta);
        theProbe.sampleNanos(delta);
    }
}