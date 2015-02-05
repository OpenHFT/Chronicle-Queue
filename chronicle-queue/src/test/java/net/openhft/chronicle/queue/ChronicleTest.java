package net.openhft.chronicle.queue;

import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireKey;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.function.Function;

import static org.junit.Assert.assertTrue;

public class ChronicleTest {

    public static final int RUNS = 1000000;

    @Test
    public void testCreateAppender() throws Exception {
        for (int r = 0; r < 2; r++) {
            for (int t = 1; t <= Runtime.getRuntime().availableProcessors(); t++) {
                List<Future<?>> futureList = new ArrayList<>();
                long start = System.nanoTime();
                for (int j = 0; j < 4; j++) {
                    String name = "single" + start + "-" + j + ".q";
                    new File(name).deleteOnExit();
                    Chronicle chronicle = new ChronicleQueueBuilder(name).build();

                    futureList.add(ForkJoinPool.commonPool().submit(() -> {
                        writeSome(chronicle);
                        return null;
                    }));
                }
                for (Future<?> future : futureList) {
                    future.get();
                }
                futureList.clear();
                long mid = System.nanoTime();
                for (int j = 0; j < 4; j++) {
                    String name = "single" + start + "-" + j + ".q";
                    new File(name).deleteOnExit();
                    Chronicle chronicle = new ChronicleQueueBuilder(name).build();

                    futureList.add(ForkJoinPool.commonPool().submit(() -> {
                        readSome(chronicle);
                        return null;
                    }));
                }
                for (Future<?> future : futureList) {
                    future.get();
                }
                long end = System.nanoTime();
                System.out.printf("Threads: %,d - Write rate %.1f M/s - Read rate %.1f M/s%n", t, t * RUNS * 1e3 / (mid - start), t * RUNS * 1e3 / (end - mid));
            }
        }
    }

    private void readSome(Chronicle chronicle) throws IOException {
        ExcerptTailer tailer = chronicle.createTailer();
        StringBuilder sb = new StringBuilder();
        // TODO check this is still needed in future versions.
        Function<WireIn, WireIn> reader = wire -> wire.read(TestKey.test).text(sb);
        for (int i = 0; i < RUNS; i++) {
            assertTrue(tailer.readDocument(reader));
        }
    }

    private void writeSome(Chronicle chronicle) throws IOException {
        ExcerptAppender appender = chronicle.createAppender();
        for (int i = 0; i < RUNS; i++) {
            appender.writeDocument(wire -> wire.write(TestKey.test).text("Hello World23456789012345678901234567890"));
        }
    }

    enum TestKey implements WireKey {
        test
    }
}