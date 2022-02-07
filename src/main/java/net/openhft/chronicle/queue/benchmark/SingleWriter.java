package net.openhft.chronicle.queue.benchmark;

import net.openhft.affinity.Affinity;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

// Test end-to-end latency for queue with single writer; single reader
// various message sizes

// mvn exec:java -Dexec.mainClass=net.openhft.chronicle.queue.benchmark.SingleWriter -Dexec.args="-cpus 2 4 -dir /dev/shm"

public class SingleWriter {

    final int COUNT = 1_000_000;
    final int WARMUP = 50_000;
    final long MSGS_PER_SECOND = 100_000L;
    final RollCycle ROLL_CYCLE = RollCycles.DAILY; // LARGE_HOURLY_SPARSE;

    final int APPENDERS = 1;

    final AtomicInteger index = new AtomicInteger(0);
    final AtomicInteger flag = new AtomicInteger(0);
    final String tmp;

    static {
        System.setProperty("jvm.resource.tracing", "false");
        System.setProperty("check.thread.safety", "false");
    }

    public SingleWriter(String dir) {
        this.tmp = dir + File.separator + "SingleWriter";
    }

    private void initialise() {
        index.set(0);
        flag.set(0);
    }

    private void spin_wait(long nanos) {
        if(nanos > 0) {
            long now = System.nanoTime();
            while(System.nanoTime() - now < nanos);
        }
    }

    private void tailer(final long MSGSIZE, final long blocksize) {
        Bytes data = Bytes.allocateDirect(MSGSIZE);
        long[] deltas = new long[APPENDERS*COUNT];

        Affinity.setAffinity(Cpus.next());

        try (ChronicleQueue cq = SingleChronicleQueueBuilder.binary(tmp)
                .blockSize(blocksize)
                .rollCycle(ROLL_CYCLE)
                .build()) {

            ExcerptTailer tailer = cq.createTailer();

            flag.set(1);
            while(flag.get() != APPENDERS + 1);

            int idx = -APPENDERS*WARMUP;
            while(idx < APPENDERS*COUNT) {
                try (DocumentContext dc = tailer.readingDocument()) {
                    if(!dc.isPresent())
                        continue;

                    Bytes bytes = dc.wire().bytes();

                    data.clear();
                    bytes.read(data, (int)MSGSIZE);

                    long startTime = data.readLong(0);
                    if(idx >= 0)
                        deltas[idx] = System.nanoTime() - startTime;

                    ++idx;
                }
            }
        }

        Arrays.sort(deltas);

        System.out.println("Message size: " + MSGSIZE);
        double[] percentiles = {50, 90, 97, 99, 99.7, 99.9, 99.97, 99.99, 99.997, 99.999};
        for(double p : percentiles)
            System.out.println(p + "%: " + deltas[(int)(p*1000*APPENDERS*COUNT/100000)]);
    }

    private void appender(final long MSGSIZE, final long blocksize) {
        Bytes data = Bytes.allocateDirect(MSGSIZE);
        for(int i=0; i<MSGSIZE; i+=8) {
            data.writeLong(i, 0);
        }

        Affinity.setAffinity(Cpus.next());

        try (ChronicleQueue cq = SingleChronicleQueueBuilder.binary(tmp)
                .blockSize(blocksize)
                .rollCycle(ROLL_CYCLE)
                .build()) {

            ExcerptAppender appender = cq.acquireAppender();

            final long nano_delay = 1_000_000_000L/MSGS_PER_SECOND;

            long[] deltas = new long[COUNT];

            while(flag.get() == 0);
            flag.incrementAndGet();

            for (int i = -WARMUP; i < COUNT; ++i) {

                long startTime = System.nanoTime();
                try (DocumentContext dc = appender.writingDocument()) {

                    Bytes bytes = dc.wire().bytes();

                    // will contain the size of the blob
                    data.writeLong(0, startTime);

                    bytes.write(data,0, MSGSIZE);
                }

                long delay = nano_delay - (System.nanoTime() - startTime);
                spin_wait(delay);
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Opts.parseArgs(args);
        Cpus.initialise();

        if(Opts.empty()) {
            System.out.println("Options:"
                    + "\n\t-cpus <A B, cpus for read/write threads. Require 2>"
                    + "\n\t-dir <queue directory>"
            );

            System.exit(0);
        }

        if(Cpus.count() < 2) {
            System.out.println("Require 2 cpus specified using the -cpus option");
            System.exit(-1);
        }

        if(!Opts.has("dir")) {
            System.out.println("Require -dir option to specify path for queue");
            System.exit(-1);
        }

        new SingleWriter(Opts.get("dir").get(0)).test();
        System.exit(0);
    }

    private void test() throws InterruptedException {
        long[] sizes = new long[]{ 16, 64, 128, 256, 512, 1024, 4096 };
        long[] blocksizes = new long[] { 256L << 20, 64L << 20 };

        IOTools.deleteDirWithFiles(tmp);

        for(int i=0; i< sizes.length; ++i) {
            final long MSGSIZE=sizes[i];

            initialise();
            ArrayList<Thread> appenders = new ArrayList<>();
            for(int a=0; a<APPENDERS; ++a) {
                final long blocksize = blocksizes[a % blocksizes.length];
                appenders.add(new Thread(() -> appender(MSGSIZE, 16L << 30 /*blocksize*/)));
            }

            Thread tailer = new Thread(()->tailer(MSGSIZE, 16L << 30 /*32L << 20*/));

            for(int a=0; a<APPENDERS; ++a)
                appenders.get(a).start();
            tailer.start();

            for(int a=APPENDERS-1; a>=0; --a)
                appenders.get(a).join();
            tailer.join();

            IOTools.deleteDirWithFiles(tmp);
        }

        System.exit(0);
    }
}
