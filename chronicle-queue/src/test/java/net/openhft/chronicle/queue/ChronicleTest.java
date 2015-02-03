package net.openhft.chronicle.queue;

import net.openhft.chronicle.wire.WireIn;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.function.Function;

import static org.junit.Assert.assertTrue;

public class ChronicleTest {

    public static final int RUNS = 2000000;

    @Test
    public void testCreateAppender() throws Exception {
        for (int t = 0; t < 10; t++) {
            String name = "/tmp/single" + System.nanoTime() + ".q";
            new File(name).deleteOnExit();
            Chronicle chronicle = new ChronicleQueueBuilder(name).build();
            long start = System.nanoTime();
            writeSome(chronicle);
            long mid = System.nanoTime();
            readSome(chronicle);
            long end = System.nanoTime();
            System.out.printf("Write time %,d - Read time %,d %n", (mid - start) / RUNS, (end - mid) / RUNS);
        }
    }

    private void readSome(Chronicle chronicle) throws IOException {
        ExcerptTailer tailer = chronicle.createTailer();
        StringBuilder sb = new StringBuilder();
        // TODO check this is still needed in future versions.
        Function<WireIn, WireIn> reader = wire -> wire.read().text(sb);
        for (int i = 0; i < RUNS; i++) {
            assertTrue(tailer.readDocument(reader));
        }
    }

    private void writeSome(Chronicle chronicle) throws IOException {
        ExcerptAppender appender = chronicle.createAppender();
        for (int i = 0; i < RUNS; i++) {
            appender.writeDocument(wire -> wire.write().text("Hello World"));
        }
    }
}