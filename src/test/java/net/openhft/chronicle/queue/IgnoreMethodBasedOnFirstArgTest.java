package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.MethodFilterOnFirstArg;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class IgnoreMethodBasedOnFirstArgTest extends QueueTestCommon {

    private static final String EXPECTED_ENVELOPE = "for:rob";
    private static final String MSG = "hello world";

    interface Printer extends MethodFilterOnFirstArg {
        void print(String envelope, String msg);
    }

    @Test
    public void testIgnoreMethodBasedOnFirstArg() {
        try (SingleChronicleQueue build = SingleChronicleQueueBuilder.binary(DirectoryUtils.tempDir("q")).build()) {
            Printer printer = build.acquireAppender().methodWriter(Printer.class);
            printer.print(EXPECTED_ENVELOPE, MSG);
            MethodReader mr = build.createTailer().methodReaderBuilder().build(
                    new Printer() {

                        @Override
                        public boolean ignoreMethodBasedOnFirstArg(final String methodName, final Object firstArg) {
                            assertEquals(EXPECTED_ENVELOPE, firstArg);
                            return false;
                        }

                        @Override
                        public void print(String envelope, final String msg) {
                            assertEquals(EXPECTED_ENVELOPE, envelope);
                            assertEquals(MSG, msg);
                        }
                    });
            mr.readOne();
        }
    }
}

