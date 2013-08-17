/*
 * Copyright 2013 Peter Lawrey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package vanilla.java.processingengine;

import net.openhft.chronicle.ChronicleConfig;
import net.openhft.chronicle.IndexedChronicle;
import net.openhft.lang.affinity.PosixJNAAffinity;
import vanilla.java.processingengine.api.*;
import vanilla.java.processingengine.testing.Histogram;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * For a latency test
 * Start first: PEMain
 * Then run: GEMain 2 false
 * When the count down is reached:  GEMain 1 false
 *
 * @author peter.lawrey
 */
/*
on a dual core i7-4500 laptop
Processed 10,000,000 events in and out in 7.5 seconds
The latency distribution was 0.3, 1.2/2.5, 3.8/9.3 us for the 1, 90/99, 99.9/99.99 %tile

on a hex core i7-3970X
Processed 5,000,000 events in and out in 10.0 seconds
The latency distribution was 0.2, 0.3/1.5, 3.1/31.4 us for the 1, 90/99, 99.9/99.99 %tile
 */
public class GWMain {
    public static final boolean WITH_BINDING;

    static {
        boolean binding = false;

        if (Runtime.getRuntime().availableProcessors() > 10) {
            try {
                PosixJNAAffinity.INSTANCE.getcpu();
                binding = true;
                System.out.println("binding: true");
            } catch (Throwable ignored) {
            }
        }
        WITH_BINDING = binding;
    }

    public static final int WARMUP = 20000; // number of events
    public static final long EVENT_SPACING = 1000L;

    public static void main(String... args) throws IOException, InterruptedException {
        if (args.length < 2) {
            System.err.print("java " + GWMain.class.getName() + " [1 or 2] {throughput}");
            System.exit(-1);
        }
        final int gwId = Integer.parseInt(args[0]);
        final boolean throughputTest = Boolean.parseBoolean(args[1]);

        final int orders = 10 * 1000 * 1000;

        String tmp = System.getProperty("java.io.tmpdir");
//        String tmp = System.getProperty("user.home");
        String gw2pePath = tmp + "/demo/gw2pe" + gwId;
        String pePath = tmp + "/demo/pe";

        // setup
        ChronicleConfig config = ChronicleConfig.DEFAULT.clone();
//        config.dataBlockSize(4 * 1024);
//        config.indexBlockSize(4 * 1024);
        IndexedChronicle gw2pe = new IndexedChronicle(gw2pePath, config);
        Gw2PeEvents gw2PeWriter = new Gw2PeWriter(gw2pe.createAppender());

        IndexedChronicle pe2gw = new IndexedChronicle(pePath, config);
        final Histogram warmup = new Histogram(100000, 100);
        final Histogram running = new Histogram(100000, 100);
        final Histogram[] times = {warmup};
        final AtomicInteger reportCount = new AtomicInteger(-WARMUP);
        Pe2GwEvents listener = new Pe2GwEvents() {
            @Override
            public void report(MetaData metaData, SmallReport smallReport) {
                if (metaData.sourceId != gwId) return;

                if (!throughputTest) {
                    if (reportCount.get() == WARMUP)
                        times[0] = running;
                    times[0].sample(metaData.inReadTimestamp7Delta * 100);
                }
                reportCount.getAndIncrement();
//                System.out.println(reportCount);
            }
        };
        final Pe2GwReader pe2GwReader = new Pe2GwReader(gwId, pe2gw.createTailer(), listener);

        // synchronize the start.
        if (gwId > 1) {
            int startTime = (int) ((System.currentTimeMillis() / 1000 - 5) % 10) + 5;
            System.out.println("Count down");
            for (int i = startTime; i > 0; i--) {
                System.out.print(i + " ");
                System.out.flush();
                //noinspection BusyWait
                Thread.sleep(1000);
            }
        }

        // In reality, this would be in the same thread.
        // A second thread is used here to ensure there is no Co-ordinated Omission
        // where the producer slows down to suit the consumer which makes delays seem far less significant.
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                while (reportCount.get() < orders) {
                    if (pe2GwReader.readOne() && reportCount.get() % 1000000 == 0)
                        System.out.println("processed " + reportCount.get());
                }
            }
        });
        t.start();
        if (WITH_BINDING)
            PosixJNAAffinity.INSTANCE.setAffinity(1L << 2);
        // run loop
        SmallCommand command = new SmallCommand();
        @SuppressWarnings("MismatchedQueryAndUpdateOfStringBuilder")
        StringBuilder clientOrderId = command.clientOrderId;

        System.out.println("Started");
        long start = System.nanoTime();
        long[] startTime = new long[orders + 1];
        startTime[0] = start;
        for (int i = -WARMUP; i < orders; i++) {
            if (i == 0)
                start = System.nanoTime();
            clientOrderId.setLength(0);
            clientOrderId.append("orderId-");
            clientOrderId.append(gwId);
            clientOrderId.append('-');
            clientOrderId.append(i);
            command.instrument = "XAU/EUR";
            command.price = 1209.41;
            command.quantity = 1000;
            command.side = (i & 1) == 0 ? Side.BUY : Side.SELL;
            gw2PeWriter.small(null, command);
            startTime[Math.abs(i)] = System.nanoTime();
            if (!throughputTest) {
                long expectedTime = start + i * EVENT_SPACING;
                while (System.nanoTime() < expectedTime) {
                    //
                }
            }
        }
        System.out.println("Received " + reportCount.get());
        t.join();
        long time = System.nanoTime() - start;
        System.out.printf("Processed %,d events in and out in %.1f seconds%n", orders, time / 1e9);
        if (!throughputTest) {
            System.out.printf("The latency distribution was %.1f, %.1f/%.1f, %.1f/%.1f us for the 1, 90/99, 99.9/99.99 %%tile%n",
                    times[0].percentile(0.01) / 1e3,
                    times[0].percentile(0.90) / 1e3,
                    times[0].percentile(0.99) / 1e3,
                    times[0].percentile(0.999) / 1e3,
                    times[0].percentile(0.9999) / 1e3
            );
        }
        gw2pe.close();
        pe2gw.close();

        PrintWriter pw = new PrintWriter("/tmp/report.csv");
        for (int i = 0; i < orders; i++) {
            long delay = startTime[i + 1] - startTime[i];
            long expected = startTime[i] - (start + (i + 1) * EVENT_SPACING);
            if (delay > 100e3 || expected > 1e6) {
                pw.printf("%d, %d, %d%n", i + 1, delay / 1000, expected / 1000);
            }
        }
        pw.close();
    }
}
