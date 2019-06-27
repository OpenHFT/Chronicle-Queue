package net.openhft.chronicle.queue.benchmark;

import net.openhft.chronicle.core.OS;

public class Main {
    static final int time = Integer.getInteger("time", 20);
    static final int size = Integer.getInteger("size", 60);
    static final String path = System.getProperty("path", OS.TMP);
    static final int throughput = Integer.parseInt("throughput", 1_000_000);

    static final boolean SAMPLING = Boolean.getBoolean("sampling");
    static final int ITERATIONS = Integer.getInteger("iterations", 60_000_000);
    static final int WARMUP = 500_000;

    public static void main(String[] args) throws InterruptedException {
        System.out.println("Writing with -Dtime=" + time + " -Dsize=" + size + " -Dpath=" + path);
        ThroughputMain.main(args);
        LatencyDistributionMain.main(args);
    }
}
