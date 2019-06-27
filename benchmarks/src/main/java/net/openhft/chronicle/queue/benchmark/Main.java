package net.openhft.chronicle.queue.benchmark;

import net.openhft.chronicle.core.OS;

/**
 * Centos 7.5 i7-7820X, using SSD.
 * Testing with -Dtime=20 -Dsize=60 -Dpath=/var/tmp -Dthroughput=1000000 -Dinterations=300000000
 * Writing 396,290,475 messages took 20.314 seconds, at a rate of 19,508,000 per second
 * Reading 396,290,475 messages took 19.961 seconds, at a rate of 19,853,000 per second
 * in: 50/90 97/99 99.7/99.9 99.97/99.99 99.997/99.999 99.9997/99.9999 - worst
 * was 0.20 / 0.22  0.23 / 0.24  1.4 / 1.6  2.2 / 2.5  3.4 / 7.6  46 / 76 - 7,730
 */
public class Main {
    static final int time = Integer.getInteger("time", 20);
    static final int size = Integer.getInteger("size", 60);
    static final String path = System.getProperty("path", OS.TMP);
    static final int throughput = Integer.getInteger("throughput", 1_000_000);

    static final boolean SAMPLING = Boolean.getBoolean("sampling");
    static final int interations = Integer.getInteger("iterations", 300_000_000);
    static final int WARMUP = 500_000;

    public static void main(String[] args) throws InterruptedException {
        System.out.println("Testing with " +
                "-Dtime=" + time + " " +
                "-Dsize=" + size + " " +
                "-Dpath=" + path + " " +
                "-Dthroughput=" + throughput + " " +
                "-Dinterations=" + interations);
        System.out.println("[Throughput]");
        ThroughputMain.main(args);
        System.out.println("\n[Latency]");
        LatencyDistributionMain.main(args);
    }
}
