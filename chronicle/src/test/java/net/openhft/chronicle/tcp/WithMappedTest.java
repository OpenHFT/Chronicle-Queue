package net.openhft.chronicle.tcp;

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;

import static org.junit.Assert.*;

/**
 * @author Rob Austin.
 */
public class WithMappedTest extends ChronicleTcpTestBase {

    @Rule
    public final TestName testName = new TestName();


    @Test
    public void testReplicationWithMapping() throws Exception {
        final int RUNS = 100;

        final String sourceBasePath = getVanillaTestPath("-source");
        final String sinkBasePath = getVanillaTestPath("-sink");

        final ChronicleTcpTestBase.PortSupplier portSupplier = new ChronicleTcpTestBase.PortSupplier();

        final Chronicle source = ChronicleQueueBuilder.vanilla(sourceBasePath)
                .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
                .build();

        final int port = portSupplier.getAndCheckPort();
        final Chronicle sink = ChronicleQueueBuilder.vanilla(sinkBasePath)
                .sink()
                .withMapping(new StatefulVanillaChronicleTest.NoOpMappingFunction()) // this is sent to the source
                .connectAddress("localhost", port)
                .build();

        try {

            final Thread at = new Thread("th-appender") {
                public void run() {
                    AffinityLock lock = AffinityLock.acquireLock();
                    try {
                        final ExcerptAppender appender = source.createAppender();
                        for (int i = 0; i < RUNS; i++) {
                            appender.startExcerpt();
                            long value = 1000000000 + i;
                            appender.append(value).append(' ');
                            appender.finish();
                        }

                        appender.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        lock.release();
                    }
                }
            };

            final Thread tt = new Thread("th-tailer") {
                public void run() {
                    AffinityLock lock = AffinityLock.acquireLock();
                    try {
                        final ExcerptTailer tailer = sink.createTailer();
                        for (int i = 0; i < RUNS; i++) {
                            long value = 1000000000 + i;
                            assertTrue(tailer.nextIndex());
                            long val = tailer.parseLong();

                            assertEquals("i: " + i, value, val);
                            assertEquals("i: " + i, 0, tailer.remaining());
                            tailer.finish();
                        }

                        tailer.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        lock.release();
                    }
                }
            };

            at.start();
            tt.start();

            at.join();
            tt.join();
        } finally {
            sink.close();
            sink.clear();

            source.close();
            source.clear();

            assertFalse(new File(sourceBasePath).exists());
            assertFalse(new File(sinkBasePath).exists());
        }
    }

}
