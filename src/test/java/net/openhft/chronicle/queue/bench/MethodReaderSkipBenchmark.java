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
package net.openhft.chronicle.queue.bench;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.NanoSampler;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import net.openhft.chronicle.jlbh.TeamCityHelper;
import net.openhft.chronicle.queue.ChronicleQueue;

import java.io.IOException;
import java.nio.file.Files;

public class MethodReaderSkipBenchmark implements JLBHTask {
    private static final String PAYLOAD = "this is a string which is here to provide a payload for the method reader to skip over";
    private static int iterations;
    private JLBH jlbh;
    private ChronicleQueue queue;
    private NanoSampler noopSampler;
    private NanoSampler wroteSampler;
    private AnInterface writer;
    private MethodReader reader;
    private int counter = 0;

    public static void main(String[] args) {
        iterations = Integer.getInteger("benchmarkIterations", 100_000);
        System.out.println("Iterations: " + iterations);
        int throughput = Integer.getInteger("benchmarkThroughput", 20_000);
        System.out.println("Throughput: " + throughput);

        JLBHOptions lth = new JLBHOptions()
                .warmUpIterations(100_000)
                .iterations(iterations)
                .throughput(throughput)
                .recordOSJitter(false).accountForCoordinatedOmission(true)
                .skipFirstRun(true)
                .runs(5)
                .jlbhTask(new MethodReaderSkipBenchmark());
        new JLBH(lth).start();
    }

    @Override
    public void init(JLBH jlbh) {
        this.jlbh = jlbh;
        this.noopSampler = jlbh.addProbe("noop");
        this.wroteSampler = jlbh.addProbe("wrote");
        String benchmarkQueuePath = System.getProperty("benchmarkQueuePath");

        if (benchmarkQueuePath != null) {
            System.out.println("Creating queue in dir: " + benchmarkQueuePath);

            IOTools.deleteDirWithFiles(benchmarkQueuePath, 10);

            queue = ChronicleQueue.single(benchmarkQueuePath);
        } else {
            System.out.println("Creating queue in temp dir");

            try {
                queue = ChronicleQueue.single(Files.createTempDirectory("temp").toString());
            } catch (IOException e) {
                throw new IORuntimeException(e);
            }
        }

        writer = queue.acquireAppender().methodWriter(AnInterface.class);
        reader = queue.createTailer().methodReader((AnotherInterface) s -> {
            throw new IllegalStateException("should not be called");
        });
    }

    @Override
    public void run(long startTimeNS) {
        final boolean writeSomething = counter++ % 2 == 0;
        if (writeSomething)
            writer.stringMethod(PAYLOAD);

        final long beforeReadNS = System.nanoTime();
        // we never expect to read anything. If wroteSomething we will have to skip over the written message.
        reader.readOne();
        final long endTimeNS = System.nanoTime();
        if (writeSomething)
            wroteSampler.sampleNanos(endTimeNS - beforeReadNS);
        else
            noopSampler.sampleNanos(endTimeNS - beforeReadNS);
        // The end to end time will not be very useful as it also contains writing time (half of the time)
        jlbh.sample(endTimeNS - startTimeNS);
    }

    @Override
    public void complete() {
        queue.close();
        TeamCityHelper.teamCityStatsLastRun(getClass().getSimpleName(), jlbh, iterations, System.out);
    }

    interface AnInterface {
        void stringMethod(String s);
    }

    interface AnotherInterface {
        void otherMethod(String s);
    }
}
