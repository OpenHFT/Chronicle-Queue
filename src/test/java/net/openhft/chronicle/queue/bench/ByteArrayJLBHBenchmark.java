/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.bench;

import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import net.openhft.chronicle.jlbh.TeamCityHelper;

public class ByteArrayJLBHBenchmark implements JLBHTask {
    private static final int MSG_THROUGHPUT = Integer.getInteger("throughput", 100_000_000);
    private static final int MSG_LENGTH = Integer.getInteger("length", 1_000_000);
    private static int iterations;
    private static byte[] bytesArr1 = new byte[MSG_LENGTH];
    private static byte[] bytesArr3 = new byte[MSG_LENGTH];
    private JLBH jlbh;

    static {
        System.setProperty("disable.thread.safety", "true");
        System.setProperty("jvm.resource.tracing", "false");
    }

    public static void main(String[] args) {
        int throughput = MSG_THROUGHPUT / MSG_LENGTH;
        int warmUp = Math.min(20 * throughput, 12_000);
        iterations = Math.min(15 * throughput, 100_000);

        JLBHOptions lth = new JLBHOptions()
                .warmUpIterations(warmUp)
                .iterations(iterations)
                .throughput(throughput)
                .recordOSJitter(false)
                .skipFirstRun(true)
                .runs(5)
                .jlbhTask(new ByteArrayJLBHBenchmark());
        new JLBH(lth).start();
    }

    @Override
    public void init(JLBH jlbh) {
        this.jlbh = jlbh;
    }

    @Override
    public void run(long startTimeNS) {
        byte[] bytesArr2 = new byte[MSG_LENGTH];

        System.arraycopy(bytesArr1, 0, bytesArr2, 0, bytesArr1.length);
        System.arraycopy(bytesArr2, 0, bytesArr3, 0, bytesArr2.length);

        jlbh.sample(System.nanoTime() - startTimeNS);
    }

    @Override
    public void complete() {
        TeamCityHelper.teamCityStatsLastRun(getClass().getSimpleName(), jlbh, iterations, System.out);
    }
}
