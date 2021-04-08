package net.openhft.chronicle.queue.main;

import net.openhft.chronicle.queue.internal.main.InternalBenchmarkMain;

/**
 * This class is using the following System Properties:
 *
 * static int throughput = Integer.getInteger("throughput", 250); // MB/s
 * static int runtime = Integer.getInteger("runtime", 300); // seconds
 * static String basePath = System.getProperty("path", OS.TMP);
 */
public final class BenchmarkMain {

    public static void main(String[] args) {
        InternalBenchmarkMain.main(args);
    }
}