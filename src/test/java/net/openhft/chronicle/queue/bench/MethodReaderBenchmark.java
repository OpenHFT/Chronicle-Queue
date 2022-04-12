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

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.NanoSampler;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import net.openhft.chronicle.jlbh.TeamCityHelper;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.LongConversion;
import net.openhft.chronicle.wire.MilliTimestampLongConverter;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static net.openhft.chronicle.queue.bench.BenchmarkUtils.join;

public class MethodReaderBenchmark implements JLBHTask {
    private static int iterations;
    private JLBH jlbh;
    private ChronicleQueue queue;
    private ExcerptTailer tailer;
    private ExcerptAppender appender;

    private NanoSampler noArgsCallSampler;
    private NanoSampler oneIntCallSampler;
    private NanoSampler oneLongCallSampler;
    private NanoSampler smallDtoCallSampler;
    private NanoSampler stringAndSmallDtoCallSampler;
    private NanoSampler bigDtoCallSampler;
    private NanoSampler stringAndBigDtoCallSampler;

    private Object writer;
    private ExecutionReportDTO nextExecutionReport;
    private OrderDTO nextOrder;

    private MethodReader reader;
    private Thread consumerThread;

    private volatile boolean stopped = false;

    public static void main(String[] args) {
        iterations = Integer.getInteger("benchmarkIterations", 100_000);
        System.out.println("Iterations: " + iterations);
        int throughput = Integer.getInteger("benchmarkThroughput", 20_000);
        System.out.println("Throughput: " + throughput);

        // disable as otherwise single GC event skews results heavily
        JLBHOptions lth = new JLBHOptions()
                .warmUpIterations(100_000)
                .iterations(iterations)
                .throughput(throughput)
                .recordOSJitter(false).accountForCoordinatedOmission(true)
                .skipFirstRun(true)
                .runs(5)
                .jlbhTask(new MethodReaderBenchmark());
        new JLBH(lth).start();
    }

    @Override
    public void init(JLBH jlbh) {
        this.jlbh = jlbh;
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

        appender = queue.acquireAppender();
        writer = appender.methodWriter(AnInterface.class);

        nextExecutionReport = new ExecutionReportDTO(ThreadLocalRandom.current());
        nextOrder = new OrderDTO(ThreadLocalRandom.current());

        consumerThread = new Thread(() -> {
            try (final AffinityLock affinityLock = AffinityLock.acquireCore()) {

                tailer = queue.createTailer().disableThreadSafetyCheck(true);

                noArgsCallSampler = jlbh.addProbe("No args call");
                oneIntCallSampler = jlbh.addProbe("One int call");
                oneLongCallSampler = jlbh.addProbe("One long call");
                smallDtoCallSampler = jlbh.addProbe("Small DTO call");
                stringAndSmallDtoCallSampler = jlbh.addProbe("String and small DTO call");
                bigDtoCallSampler = jlbh.addProbe("Big DTO call");
                stringAndBigDtoCallSampler = jlbh.addProbe("String and big DTO call");

                final AnInterfaceSamplingImpl samplingImpl = new AnInterfaceSamplingImpl();
                reader = tailer.methodReader(samplingImpl);

                while (!stopped) {
                    if (reader.readOne()) {
                        String startNsString;
                        do {
                            startNsString = tailer.readText();
                        }
                        while (startNsString == null);

                        long startNs = Long.parseLong(startNsString);

                        samplingImpl.doSample(startNs);

                        jlbh.sample(System.nanoTime() - startNs);
                    }
                }
            }
        });
        consumerThread.start();

    }

    @Override
    public void run(long startTimeNS) {
        AnInterface w = ((AnInterface) writer);
        final ThreadLocalRandom r = ThreadLocalRandom.current();

        int nextMethod = r.nextInt(7);

        switch (nextMethod) {
            case 0:
                w.noArgsCall();
                break;

            case 1:
                w.oneIntCall(r.nextInt());
                break;

            case 2:
                w.oneLongCall(r.nextLong());
                break;

            case 3:
                w.smallDtoCall(nextOrder);
                nextOrder = new OrderDTO(r);
                break;

            case 4:
                w.stringAndSmallDtoCall(nextSymbol(r), nextOrder);
                nextOrder = new OrderDTO(r);
                break;

            case 5:
                w.bigDtoCall(nextExecutionReport);
                nextExecutionReport = new ExecutionReportDTO(r);
                break;

            case 6:
                w.stringAndBigDtoCall(nextSymbol(r), nextExecutionReport);
                nextExecutionReport = new ExecutionReportDTO(r);
                break;

            default:
                throw new IllegalStateException("unknown method");
        }

        appender.writeText(String.valueOf(startTimeNS));
    }

    @Override
    public void complete() {
        stopped = true;
        queue.close();
        join(consumerThread);
        TeamCityHelper.teamCityStatsLastRun(getClass().getSimpleName(), jlbh, iterations, System.out);
    }

    interface AnInterface {
        void noArgsCall();

        void oneIntCall(int x);

        void oneLongCall(long x);

        void smallDtoCall(OrderDTO orderDTO);

        void stringAndSmallDtoCall(String s, OrderDTO orderDTO);

        void bigDtoCall(ExecutionReportDTO executionReportDTO);

        void stringAndBigDtoCall(String s, ExecutionReportDTO executionReportDTO);
    }

    class AnInterfaceSamplingImpl implements AnInterface {
        private long nanos;
        private NanoSampler sampler;

        @Override
        public void noArgsCall() {
            nanos = System.nanoTime();
            sampler = noArgsCallSampler;
        }

        @Override
        public void oneIntCall(int x) {
            nanos = System.nanoTime();
            sampler = oneIntCallSampler;
        }

        @Override
        public void oneLongCall(long x) {
            nanos = System.nanoTime();
            sampler = oneLongCallSampler;
        }

        @Override
        public void smallDtoCall(OrderDTO orderDTO) {
            nanos = System.nanoTime();
            sampler = smallDtoCallSampler;
        }

        @Override
        public void stringAndSmallDtoCall(String s, OrderDTO orderDTO) {
            nanos = System.nanoTime();
            sampler = stringAndSmallDtoCallSampler;
        }

        @Override
        public void bigDtoCall(ExecutionReportDTO executionReportDTO) {
            nanos = System.nanoTime();
            sampler = bigDtoCallSampler;
        }

        @Override
        public void stringAndBigDtoCall(String s, ExecutionReportDTO executionReportDTO) {
            nanos = System.nanoTime();
            sampler = stringAndBigDtoCallSampler;
        }

        public void doSample(long startNs) {
            sampler.sampleNanos(nanos - startNs);
        }
    }

    static class OrderDTO extends SelfDescribingMarshallable {
        private char side;
        private char ordType;
        private String symbol;
        private long accountId;
        private double orderQty;
        private double price;
        private long createdTime;

        public OrderDTO(Random r) {
            side = (char) r.nextInt();
            ordType = (char) r.nextInt();
            symbol = nextSymbol(r);
            accountId = r.nextLong();
            orderQty = r.nextDouble();
            price = r.nextDouble();
            createdTime = nextTimestampMillis(r);
        }
    }

    static class ExecutionReportDTO extends SelfDescribingMarshallable {
        private String orderID;
        private Bytes<?> clOrdID;
        private String execID;
        private char execTransType;
        private char execType;
        private char ordStatus;
        private Bytes<?> account;
        private char settlmntTyp;
        private Bytes<?> securityID;
        private String idSource;
        private char side;
        private double orderQty;
        private char ordType;
        private double price;
        private String currency;
        private char timeInForce;
        private double lastShares;
        private double lastPx;
        private String lastMkt;
        private double leavesQty;
        private double cumQty;
        private double avgPx;
        private String tradeDate;
        @LongConversion(MilliTimestampLongConverter.class)
        private long transactTime;
        private String settlCurrency;
        private char handlInst;
        private long createdNS;

        public ExecutionReportDTO(Random r) {
            orderID = nextSymbol(r);
            clOrdID = Bytes.from(nextSymbol(r));
            execID = nextSymbol(r);
            execTransType = (char) r.nextInt();
            execType = (char) r.nextInt();
            ordStatus = (char) r.nextInt();
            account = Bytes.from(nextSymbol(r));
            settlmntTyp = (char) r.nextInt();
            securityID = Bytes.from(nextSymbol(r));
            idSource = nextSymbol(r);
            side = (char) r.nextInt();
            orderQty = r.nextDouble();
            ordType = (char) r.nextInt();
            price = r.nextDouble();
            currency = nextSymbol(r);
            timeInForce = (char) r.nextInt();
            lastShares = r.nextDouble();
            lastPx = r.nextDouble();
            lastMkt = nextSymbol(r);
            leavesQty = r.nextDouble();
            cumQty = r.nextDouble();
            avgPx = r.nextDouble();
            tradeDate = nextSymbol(r);
            transactTime = nextTimestampMillis(r);
            settlCurrency = nextSymbol(r);
            handlInst = (char) r.nextInt();
            createdNS = nextTimestampMillis(r);
        }
    }

    private static String nextSymbol(Random r) {
        StringBuilder sb = new StringBuilder();
        final int length = r.nextInt(6) + 2;
        for (int i = 0; i < length + 2; i++) {
            sb.append('A' + r.nextInt(25));
        }
        return sb.toString();
    }

    private static long nextTimestampMillis(Random r) {
        return System.currentTimeMillis() + r.nextInt(1_000_000) - 500_000;
    }
}
