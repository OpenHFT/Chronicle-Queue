package net.openhft.chronicle.queue.impl.single;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import static java.lang.ProcessBuilder.Redirect.INHERIT;
import static java.lang.String.format;
import static java.lang.System.gc;
import static java.lang.System.nanoTime;
import static java.lang.management.ManagementFactory.getOperatingSystemMXBean;
import static java.lang.management.ManagementFactory.getRuntimeMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import static net.openhft.chronicle.queue.RollCycles.TEST_SECONDLY;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import net.openhft.chronicle.wire.UnrecoverableTimeoutException;
import static net.openhft.chronicle.wire.WireType.BINARY_LIGHT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Resource leaks makes the queue to open more and more files. 
 */
public class TooManyOpenFilesTest {

    private static final Logger logger = getLogger(TooManyOpenFilesTest.class);

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setUp() throws IOException {
        OperatingSystemMXBean os = getOperatingSystemMXBean();
        assumeFalse(os.getName().contains("Windows"));

        RuntimeMXBean runtime = getRuntimeMXBean();
        String name = runtime.getName();
        pid = name.substring(0, name.indexOf("@"));
        queueDir = temporaryFolder.newFolder("leaks");
    }

    private String pid;

    private File queueDir;

    @Test
    public void test() throws IOException, InterruptedException {
        
        int targetLeakCount = 100;
        try (SingleChronicleQueue queue = binary(queueDir)
                .wireType(BINARY_LIGHT)
                .rollCycle(TEST_SECONDLY).build()) {
            for (int i = 0; i < (targetLeakCount + 1) / 2; i++) {
                writeEvents(queue);
                emulateRunningApplication();
                readEvents(queue);
                emulateRunningApplication();
            }
        }

        int leakCount = 0;
        Process lsof = new ProcessBuilder()
                .command("lsof", "-p", pid)
                .redirectError(INHERIT)
                .redirectInput(INHERIT)
                .start();
        try (BufferedReader input = new BufferedReader(new InputStreamReader(lsof.getInputStream()))) {
            while (true) {
                String line = input.readLine();
                if (line == null) {
                    break;
                }
                if (line.contains("leaks")) {
                    logger.info(line);
                    leakCount++;
                }
            }
        }
        
        lsof.waitFor();
        logger.info("Found {} leaks", leakCount);
        
        // test is correct
        assertTrue(leakCount >= targetLeakCount);
        
        // queue has leaks
        assertEquals(0, leakCount);
    }

    private static void writeEvents(final SingleChronicleQueue queue) throws UnrecoverableTimeoutException {
        ExcerptAppender app = queue.acquireAppender();
        int n = 1;
        for (int i = 0; i < n; i++) {
            int index = i;
            app.writeBytes(bytes -> {
                bytes.writeInt(index);
                bytes.writeLong(nanoTime());
            });
        }
        logger.info("Written {} events", n);
    }

    private static void readEvents(final SingleChronicleQueue queue) {
        ExcerptTailer tailer = queue.createTailer();
        int readCount = 0;
        while (tailer.readBytes(bytes -> {
            int index = bytes.readInt();
            long nanos = bytes.readLong();
            logger.trace(format("#%03d: %08x", index, nanos));
        })) {
            readCount++;
        }
        logger.info("Read {} events", readCount);
    }

    private static void emulateRunningApplication() {
        gc();
    }

}
