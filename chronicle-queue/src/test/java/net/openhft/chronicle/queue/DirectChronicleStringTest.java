package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.DirectChronicle;
import net.openhft.chronicle.wire.WireKey;
import net.openhft.lang.io.Bytes;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

/**
 * using direct chronicle to send a string
 */
public class DirectChronicleStringTest {

    public static final int RUNS = 1000000;
    public static final String EXPECTED_STRING = "Hello World23456789012345678901234567890";

    @Test
    public void testCreateAppender() throws Exception {
        for (int r = 0; r < 2; r++) {
            for (int t = 1; t <= Runtime.getRuntime().availableProcessors(); t++) {
                List<Future<?>> futureList = new ArrayList<>();
                long start = System.nanoTime();
                for (int j = 0; j < 4; j++) {
                    String name = "single" + start + "-" + j + ".q";
                    new File(name).deleteOnExit();
                    DirectChronicle chronicle = (DirectChronicle) new ChronicleQueueBuilder(name)
                            .build();

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
                    DirectChronicle chronicle = (DirectChronicle) new ChronicleQueueBuilder(name)
                            .build();

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

    private void readSome(DirectChronicle chronicle) throws IOException {
        final Bytes tailer = chronicle.bytes();
        for (int i = 0; i < RUNS; i++) {
            assertEquals(EXPECTED_STRING, tailer.readUTF());
        }
    }

    private void writeSome(DirectChronicle chronicle) throws IOException {

        final Bytes appender = chronicle.bytes();
        for (int i = 0; i < RUNS; i++) {
            appender.writeUTF(EXPECTED_STRING);
        }
    }


    enum TestKey implements WireKey {
        test
    }
}