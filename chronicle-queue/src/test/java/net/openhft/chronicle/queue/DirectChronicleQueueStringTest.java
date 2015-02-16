package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.DirectChronicleQueue;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.DirectStore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * using direct chronicle to send a string
 */
/*
Threads: 1 - Write rate 24.4 M/s - Read rate 34.1 M/s
Threads: 2 - Write rate 31.8 M/s - Read rate 59.3 M/s
Threads: 3 - Write rate 47.3 M/s - Read rate 90.1 M/s
Threads: 4 - Write rate 62.8 M/s - Read rate 117.9 M/s
Threads: 5 - Write rate 77.5 M/s - Read rate 145.6 M/s
Threads: 6 - Write rate 92.0 M/s - Read rate 161.0 M/s
Threads: 7 - Write rate 107.2 M/s - Read rate 196.5 M/s
Threads: 8 - Write rate 120.2 M/s - Read rate 221.3 M/s
Threads: 9 - Write rate 136.8 M/s - Read rate 244.3 M/s
Threads: 10 - Write rate 143.6 M/s - Read rate 268.7 M/s
Threads: 11 - Write rate 161.7 M/s - Read rate 260.8 M/s
 */
public class DirectChronicleQueueStringTest {

    public static final int RUNS = 1000000;
    public static final String EXPECTED_STRING = "Hello World23456789012345678901234567890";
    public static final byte[] EXPECTED_BYTES = EXPECTED_STRING.getBytes();
    public static final String TMP = new File("/tmp").isDirectory() ? "/tmp" : System.getProperty("java.io.tmpdir");
    private static final Logger LOG = LoggerFactory.getLogger(DirectChronicleQueueStringTest.class.getName());

    @Test
    public void testCreateAppender() throws Exception {
        for (int r = 0; r < 2; r++) {
            for (int t = 1; t < Runtime.getRuntime().availableProcessors(); t++) {
                List<Future<?>> futureList = new ArrayList<>();

                List<File> files = new ArrayList<>();
                long start = System.nanoTime();
                for (int j = 0; j < t; j++) {
                    String name = TMP + "/single" + start + "-" + j + ".q";
                    File file = new File(name);
                    file.deleteOnExit();
                    files.add(file);
                    DirectChronicleQueue chronicle = (DirectChronicleQueue) new ChronicleQueueBuilder(name)
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
                for (int j = 0; j < t; j++) {
                    String name = TMP + "/single" + start + "-" + j + ".q";
                    new File(name).deleteOnExit();
                    DirectChronicleQueue chronicle = (DirectChronicleQueue) new ChronicleQueueBuilder(name)
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
                for (File f : files) {
                    f.delete();
                }
            }
        }
    }

    private void readSome(DirectChronicleQueue chronicle) throws IOException {
        try (DirectStore allocate = DirectStore.allocate(EXPECTED_BYTES.length)) {
            final Bytes toRead = allocate.bytes();
            AtomicLong offset = new AtomicLong(chronicle.firstBytes());
            for (int i = 0; i < RUNS; i++) {
                toRead.clear();
                chronicle.readDocument(offset, toRead);
            }
        } catch (Exception e) {
            LOG.error("", e);
        }
    }

    private void writeSome(DirectChronicleQueue chronicle) throws IOException {
        try (DirectStore allocate = DirectStore.allocate(EXPECTED_BYTES.length)) {
            final Bytes toWrite = allocate.bytes();
            toWrite.write(EXPECTED_BYTES);
            for (int i = 0; i < RUNS; i++) {
                toWrite.clear();
                chronicle.appendDocument(toWrite);
            }
        } catch (Exception e) {
            LOG.error("", e);
        }
    }
}