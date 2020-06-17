package net.openhft.chronicle.queue.benchmark;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.BufferMode;

/**
 * Centos 7.5 i7-7820X, using SSD.
 * Testing with -Dtime=20 -Dsize=60 -Dpath=/var/tmp -Dthroughput=1000000 -Dinterations=300000000
 * Writing 396,290,475 messages took 20.314 seconds, at a rate of 19,508,000 per second
 * Reading 396,290,475 messages took 19.961 seconds, at a rate of 19,853,000 per second
 * in: 50/90 97/99 99.7/99.9 99.97/99.99 99.997/99.999 99.9997/99.9999 - worst
 * was 0.20 / 0.22  0.23 / 0.24  1.4 / 1.6  2.2 / 2.5  3.4 / 7.6  46 / 76 - 7,730
 * <p>
 * Centos 7.6, Gold 6254 tmpfs
 * Writing 530,451,691 messages took 20.030 seconds, at a rate of 26,483,000 per second
 * Reading 530,451,691 messages took 32.158 seconds, at a rate of 16,495,000 per second
 * in: 50/90 97/99 99.7/99.9 99.97/99.99 99.997/99.999 99.9997/99.9999 - worst
 * was 0.22 / 0.28  0.30 / 0.34  0.94 / 1.1  2.0 / 3.3  16 / 40  225 / 336 - 3,210 (same socket)
 * was 0.30 / 0.34  0.36 / 0.38  0.94 / 1.1  2.6 / 3.5  16 / 48  270 / 369 - 3,210 (diff socket)
 * <p>
 * Centos 7.6, Gold 6254 SSD
 * <p>
 * in: 50/90 97/99 99.7/99.9 99.97/99.99 99.997/99.999 99.9997/99.9999 - worst
 * was 0.21 / 0.28  0.33 / 0.36  1.2 / 1.8  3.4 / 5.5  17 / 168  451 / 573 - 59,770
 * in: 50/90 97/99 99.7/99.9 99.97/99.99 99.997/99.999 99.9997/99.9999 - worst
 * was 0.34 / 0.42  0.44 / 0.78  2.1 / 5.2  7.0 / 9.5  12 / 68  176 / 500 - 34,600
 */
public class Main {
    static final int time = Integer.getInteger("time", 20);
    static final int size = Integer.getInteger("size", 60);
    static final String path = System.getProperty("path", OS.TMP);
    static final int throughput = Integer.getInteger("throughput", 1_000_000);
    static final int threads = Integer.getInteger("threads", 1);

    static final boolean fullWrite = Jvm.getBoolean("fullWrite");
    static final boolean SAMPLING = Jvm.getBoolean("sampling");
    static final int interations = Integer.getInteger("iterations", 300_000_000);
    static final BufferMode BUFFER_MODE = getBufferMode();

    static {
        System.out.println("Testing with " +
                "-Dtime=" + time + " " +
                "-Dthreads=" + threads + " " +
                "-Dsize=" + size + " " +
                "-Dpath=" + path + " " +
                "-Dthroughput=" + throughput + " " +
                "-Dinterations=" + interations + " " +
                "-DbufferMode=" + BUFFER_MODE);
    }

    static final int WARMUP = 500_000;

    private static BufferMode getBufferMode() {
        String bufferMode = System.getProperty("bufferMode");
        if (bufferMode != null && bufferMode.length() > 0)
            return BufferMode.valueOf(bufferMode);
        BufferMode bm;
        try {
            Class.forName("software.chronicle.enterprise.queue.ChronicleRingBuffer");
            bm = BufferMode.Asynchronous;
        } catch (ClassNotFoundException cnfe) {
            bm = BufferMode.None;
        }
        return bm;
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("[Throughput]");
        ThroughputMain.main(args);
        System.out.println("\n[Latency]");
        LatencyDistributionMain.main(args);
    }
}
