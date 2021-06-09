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

import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import net.openhft.chronicle.jlbh.TeamCityHelper;

public class ByteArrayJLBHBenchmark implements JLBHTask {
    private static final int MSG_THROUGHPUT = Integer.getInteger("throughput", 100_000_000);
    private static final int MSG_LENGTH = Integer.getInteger("length", 1_000_000);
    private static int iterations;
    static byte[] bytesArr1 = new byte[MSG_LENGTH];
    static byte[] bytesArr3 = new byte[MSG_LENGTH];
    private JLBH jlbh;

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
