package net.openhft.chronicle.queue.benchmark;

import net.openhft.affinity.Affinity;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

// measure write, read throughput (max rate, no pause)
// mvn exec:java -Dexec.mainClass=net.openhft.chronicle.queue.benchmark.Throughput -Dexec.args="-cpus 2 4 -dir /dev/shm"

public class Throughput {

    private final static ArrayList<Integer> msgsizes = new ArrayList<>(Arrays.asList(16,64,128,256,512,1024,2048,4096));
    private final static long BLOCKSIZE = 8L << 30;
    private final String dir;

    private int COUNT;
    double[] writes;
    double[] reads;

    private int MSGSIZE;
    Bytes src;

    double WRITES_PER_S;
    double READS_PER_S;

    static {
        System.setProperty("jvm.resource.tracing", "false");
        System.setProperty("check.thread.safety", "false");
    }

    private void write() {

        Random random = new Random();
        for(int i=0; i<MSGSIZE; ++i)
            src.writeByte((byte)random.nextInt(128));

        try(ChronicleQueue queue = SingleChronicleQueueBuilder.binary(dir)
                .blockSize(BLOCKSIZE)
                .rollCycle(RollCycles.DAILY)
                .build()) {
            Affinity.setAffinity(Cpus.next());
            ExcerptAppender appender = queue.acquireAppender();

            long start = System.nanoTime();
            int idx = 0;
            for(int i=1; i<=COUNT; ++i) {
                try(DocumentContext dc = appender.writingDocument()) {
                    dc.wire().bytes().write(src, 0L, MSGSIZE);
                }

                if((i%1_000_000) == 0) {
                    long end = System.nanoTime();
                    double delta = (double)(end-start)/1_000_000_000;
                    writes[idx++] = 1.0/delta;
                    start = end;
                }
            }
        }

        Arrays.sort(writes);
        WRITES_PER_S = writes[writes.length*50/100];
    }

    private void read() throws IOException {
        Bytes rcv = Bytes.allocateDirect(MSGSIZE);
        for(int i=0; i<MSGSIZE; ++i)
            rcv.writeByte((byte)0);

        try(ChronicleQueue queue = SingleChronicleQueueBuilder.binary(dir)
                .blockSize(BLOCKSIZE)
                .rollCycle(RollCycles.DAILY)
                .build()) {
            Affinity.setAffinity(Cpus.next());
            ExcerptTailer tailer = queue.createTailer();

            long start = System.nanoTime();
            int idx = 0;
            for(int i=1; i<=COUNT; ) {

                rcv.clear();

                try(DocumentContext dc = tailer.readingDocument()) {
                    dc.wire().bytes().read(rcv, MSGSIZE);
                }

                if((i%1_000_000) == 0) {
                    long end = System.nanoTime();
                    double delta = (double)(end-start)/1_000_000_000;
                    reads[idx++] = 1.0/delta;
                    start = end;
                }

                ++i;
            }
        }

        Arrays.sort(reads);
        READS_PER_S = reads[reads.length*50/100];
    }

    private Throughput(String dir) {
        this.dir =  dir + File.separator + "Throughput";
    }

    private void run() throws IOException {
        for(int msgsize : msgsizes) {
            System.out.println("\n---------- Start: msg size = " + msgsize + " bytes ----------");
            {
                IOTools.deleteDirWithFiles(dir);
                MSGSIZE = msgsize;
                src = Bytes.allocateDirect(MSGSIZE);

                // cap at 50GB
                long count = 50_000_000_000L / MSGSIZE;
                count = (count / 1_000_000) * 1_000_000;

                COUNT = count > 80_000_000L ? 80_000_000 : (int)count;
                writes = new double[COUNT / 1_000_000];
                reads = new double[COUNT / 1_000_000];

                write();
                read();

                System.out.printf("Samples: %d, msg size: %d, writes/s (M): %.2f, reads/s (M): %.2f\n", COUNT, MSGSIZE, WRITES_PER_S, READS_PER_S);
            }
            System.out.println("---------- End: msg size = " + msgsize + " bytes ----------\n");
        }
        IOTools.deleteDirWithFiles(dir);
    }

    public static void main(String[] args) throws IOException {
        Opts.parseArgs(args);
        Cpus.initialise();

        if(Opts.empty()) {
            System.out.println("Options:"
                    + "\n\t-cpus <A B, cpus for read/write threads. Require 2>"
                    + "\n\t-dir <queue directory>"
            );

            System.exit(0);
        }

        if(Cpus.count() < 2)
            throw new IllegalArgumentException("Not enough reserved CPUs (" + Cpus.count() + ") for requested number of readers. Require 2");

        if(!Opts.has("dir")) {
            System.out.println("Require -dir option to specify path for queue");
            System.exit(-1);
        }

        new Throughput(Opts.get("dir").get(0)).run();
        System.exit(0);
    }
}
