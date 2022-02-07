package net.openhft.chronicle.queue.benchmark;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Structure;
import net.openhft.affinity.Affinity;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

// Test end-to-end latency for queue with multiple concurrent writers; single reader
// 1 queue written by all N writers; read by 1 reader

// mvn exec:java -Dexec.mainClass=net.openhft.chronicle.queue.benchmark.MultiWriter -Dexec.args="-cpus 2 3 -w 5 -dir /dev/shm"

public class MultiWriter implements Closeable {

    private final long BLOCKSIZE = 8L << 30;
    private final long WARMUP = 50_000;

    private final long TOTAL_MSGS_PER_SECOND;
    private final long RUNTIME_S;
    private final int MSG_JITTER_PERCENT;

    private final long MSGS_PER_SECOND_PER_WRITER;
    private final long MSG_PERIOD_NS_PER_WRITER;
    private final long MSGS_PER_WRITER;
    private boolean BUSY_WAIT=false;

    private final String dir;
    private final AtomicInteger flag = new AtomicInteger();

    private int MSGSIZE;
    private final int WRITERS;

    ThreadLocal<NanoTime> nanoTimeTL = ThreadLocal.withInitial(NanoTime::new);

    static {
        System.setProperty("jvm.resource.tracing", "false");
        System.setProperty("check.thread.safety", "false");
    }

    /**
     * Linux supports a nanosleep call which is reasonably reliable for sleeps down to 100us without busy waiting
     */
    public static class NanoTime {

        private final timespec ts = new timespec();
        private final timespec tr = new timespec();

        @Structure.FieldOrder({"tv_sec", "tv_nsec"})
        public static class timespec extends Structure {
            public long tv_sec;
            public long tv_nsec;
        }

        public void nanosleep(long nanos) {
            ts.tv_sec = 0;
            ts.tv_nsec = nanos;

            CLibrary.INSTANCE.nanosleep(ts, tr);
        }

        interface CLibrary extends Library {
            NanoTime.CLibrary INSTANCE = Native.load("c", CLibrary.class);

            int nanosleep(final timespec req, final timespec rem);
        }
    }

    /**
     * Add some randomised jitter for each writer between appends
     */
    private long delay(Random random) {
        long fuzzy = MSG_PERIOD_NS_PER_WRITER * MSG_JITTER_PERCENT / 100;
        return MSG_PERIOD_NS_PER_WRITER - (fuzzy / 2) + random.nextInt((int) (fuzzy));
    }

    /**
     * pause the calling thread for the given number of nanos (reliable for >~ 100us delays)
     */
    private void nanosleep(long nanos) {
        if(BUSY_WAIT) {
            long now = System.nanoTime();
            while(System.nanoTime() - now < nanos);
        } else {
            nanoTimeTL.get().nanosleep(nanos);
        }
    }

    private void append_loop(ExcerptAppender appender, Bytes src, long N, Runnable pauser) {
        for (int i = 0; i < N; ++i) {
            src.writeLong(0, System.nanoTime());
            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().bytes().write(src, 0L, MSGSIZE);
            }

            pauser.run();
        }
    }

    private void write(int writer) {

        final Bytes src = Bytes.allocateDirect(MSGSIZE);

        final Random random = new Random();
        for (int i = 0; i < MSGSIZE; ++i)
            src.writeByte((byte) random.nextInt(128));

        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(dir)
                .blockSize(BLOCKSIZE)
                .rollCycle(RollCycles.DAILY)
                .build()) {

            if(BUSY_WAIT) {
                int core = Cpus.get(1 + (writer % (Cpus.count() - 1))); // user should add gaps between cores on Ryzen
                System.out.println("Pinning writer " + writer + " to core " + core);
                Affinity.setAffinity(core); // 0th cpu for reader. spread the writers over the remainder
            }

            final ExcerptAppender appender = queue.acquireAppender();

            // announce self and wait for everyone else to initialise
            flag.incrementAndGet();
            while (flag.get() != 1 + WRITERS) ;

            // warmup
            append_loop(appender, src, WARMUP, () -> nanosleep(10_000));

            flag.incrementAndGet();
            while (flag.get() != 2 * (1 + WRITERS)) ;

            append_loop(appender, src, MSGS_PER_WRITER, () -> nanosleep(delay(random)));
        }
    }

    private void tailer_loop(ExcerptTailer tailer, Bytes rcv, long N, BiConsumer<Integer, Bytes> action) {
        for (int i = 0; i < N; ) {
            rcv.clear();

            try (DocumentContext dc = tailer.readingDocument()) {
                if (!dc.isPresent()) continue;
                dc.wire().bytes().read(rcv, MSGSIZE);
            }

            action.accept(i, rcv);

            ++i;
        }
    }

    private void read() {
        final long COUNT = MSGS_PER_WRITER * WRITERS;
        final long WARMUP_COUNT = WARMUP * WRITERS;

        final long[] deltas = new long[(int) (COUNT)];

        final Bytes rcv = Bytes.allocateDirect(MSGSIZE);

        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(dir)
                .blockSize(BLOCKSIZE)
                .rollCycle(RollCycles.DAILY)
                .build()) {
            int core = Cpus.get(0);
            System.out.println("Pinning reader to core " + core);
            Affinity.setAffinity(core);
            final ExcerptTailer tailer = queue.createTailer();

            // announce self and wait for everyone else to initialise
            flag.incrementAndGet();
            while (flag.get() != 1 + WRITERS) ;

            // warmup
            tailer_loop(tailer, rcv, WARMUP_COUNT, (i, b) -> {
            });

            flag.incrementAndGet();
            while (flag.get() != 2 * (1 + WRITERS)) ;

            tailer_loop(tailer, rcv, COUNT, (i, b) -> deltas[i] = System.nanoTime() - b.readLong(0));
        }

        Arrays.sort(deltas);
        System.out.println("Message size: " + MSGSIZE);
        double[] percentiles = {50, 90, 97, 99, 99.7, 99.9, 99.97, 99.99, 99.997, 99.999};
        for(double p : percentiles)
            System.out.println(p + "%: " + deltas[(int)(p*1000*COUNT/100000)]);
    }

    private MultiWriter(String dir, int writers, int totalMsgsPerSecond, int msgSize, int runtimeSeconds, int jitterPercent, boolean busyWait) {
        this.dir =  dir + File.separator + "MultiWriters";
        this.TOTAL_MSGS_PER_SECOND = totalMsgsPerSecond;
        this.RUNTIME_S = runtimeSeconds;
        this.MSG_JITTER_PERCENT = jitterPercent;
        this.WRITERS = writers;
        this.MSGS_PER_SECOND_PER_WRITER = TOTAL_MSGS_PER_SECOND / writers;
        this.MSG_PERIOD_NS_PER_WRITER = 1_000_000_000 / MSGS_PER_SECOND_PER_WRITER;
        this.MSGS_PER_WRITER = RUNTIME_S * MSGS_PER_SECOND_PER_WRITER;
        this.MSGSIZE = msgSize;
        this.BUSY_WAIT = busyWait;

        if (!BUSY_WAIT && MSGS_PER_SECOND_PER_WRITER > 10000)
            throw new IllegalArgumentException("No more than 10k msgs/s per writer (due to no-spin sleep resolution)");

        System.out.println("Running with " + writers + " writers, " + msgSize + " byte msgs at " + totalMsgsPerSecond + " msgs/s (total) for approx " + runtimeSeconds + "s; " + MSGS_PER_SECOND_PER_WRITER + " msgs/s per writer");
    }

    private void run() throws InterruptedException {
        IOTools.deleteDirWithFiles(dir);

        {
            flag.set(0);

            Thread reader = new Thread(this::read);
            Thread[] writers = new Thread[WRITERS];

            reader.start();

            for (int w = 0; w < WRITERS; ++w) {
                final int wfinal = w;
                writers[w] = new Thread(() -> write(wfinal));
                writers[w].start();
            }

            for (int r = 0; r < WRITERS; ++r)
                writers[r].join();

            reader.join();
        }

        IOTools.deleteDirWithFiles(dir);
    }

    public static void main(String[] args) throws InterruptedException {
        Opts.parseArgs(args);
        Cpus.initialise();

        if(Opts.empty()) {
            System.out.println("Options:"
                    + "\n\t-m <total messages per second, default 10000. Higher values require -busy option>"
                    + "\n\t-t <run time (seconds), default = 10>"
                    + "\n\t-j <% jitter between writers, default = 25>"
                    + "\n\t-s <message size (bytes), default = 256>"
                    + "\n\t-w <number of writers>"
                    + "\n\t-cpus <A B C..., cpus for read/write threads. Require 1 per writer if -busy>"
                    + "\n\t-dir <queue directory>"
                    + "\n\t-busy busy wait rather than sleep"
            );

            System.exit(0);
        }

        int writers = 1;
        {
            List<String> r = Opts.get("w");
            if (r != null && !r.isEmpty()) {
                writers = Integer.parseInt(r.get(0));
            }

            if(writers < 1) {
                System.out.println("Number of writers should be >= 1");
                System.exit(-1);
            }
        }

        int totalMsgsPerSecond = Integer.parseInt(Opts.getOrDefault("m", "10000"));
        int runTime = Integer.parseInt(Opts.getOrDefault("t", "10"));
        int jitterPercent = Integer.parseInt(Opts.getOrDefault("j", "25"));
        int msgSize = Integer.parseInt(Opts.getOrDefault("s", "256"));
        boolean busyWait = Opts.has("busy");

        int requiredCPUs = 1 + (busyWait ? writers : 0);
        if(Cpus.count() < requiredCPUs) {
            System.out.println("Too few CPUS. Need " + (1+writers) + ", have " + Cpus.count());
            System.exit(-1);
        }

        if(!Opts.has("dir")) {
            System.out.println("Require -dir option to specify path for queue");
            System.exit(-1);
        }

        try (MultiWriter t = new MultiWriter(
                Opts.get("dir").get(0),
                writers,
                totalMsgsPerSecond,
                msgSize,
                runTime,
                jitterPercent,
                busyWait)) {
            t.run();
        }

        System.exit(0);
    }

    @Override
    public void close() {
        IOTools.deleteDirWithFiles(dir);
    }

    @Override
    public boolean isClosed() {
        return false;
    }
}
