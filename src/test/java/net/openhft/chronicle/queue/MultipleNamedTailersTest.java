package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class MultipleNamedTailersTest {
    private final boolean empty;

    public MultipleNamedTailersTest(boolean empty) {
        this.empty = empty;
    }

    @Parameterized.Parameters(name = "empty={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{false},
                new Object[]{true}
        );
    }

    @Test
    public void multipleSharedTailers() {
        File tmpDir = new File(OS.getTarget(), "multipleTailers" + System.nanoTime());

        try (ChronicleQueue q1 = SingleChronicleQueueBuilder.single(tmpDir).testBlockSize()
                .rollCycle(RollCycles.TEST_SECONDLY)
                .build();
             ExcerptAppender appender = q1.acquireAppender()) {

            if (!empty)
                appender.writeText("0");

            try (ExcerptTailer namedTailer1 = q1.createTailer("named");
                 ChronicleQueue q2 = SingleChronicleQueueBuilder.single(tmpDir).testBlockSize().build();
                 ExcerptTailer namedTailer2 = q2.createTailer("named")) {
                for (int i = 0; i < 3_000; i++) {
                    final String id0 = "" + i;
                    if (i > 0 || empty)
                        appender.writeText(id0);
                    long index0 = appender.lastIndexAppended();
                    if (i % 2 == 0)
                        check(namedTailer1, id0, index0);
                    else
                        check(namedTailer2, id0, index0);
                    Jvm.pause(1);
                }
            }
        }
        IOTools.deleteDirWithFiles(tmpDir);
    }

    @Test
    public void multipleTailers() {
        File tmpDir = new File(OS.getTarget(), "multipleTailers" + System.nanoTime());

        try (ChronicleQueue q1 = SingleChronicleQueueBuilder.single(tmpDir).testBlockSize()
                .rollCycle(RollCycles.TEST_SECONDLY)
                .build();
             ExcerptAppender appender = q1.acquireAppender()) {

            if (!empty)
                appender.writeText("0");

            try (ExcerptTailer tailer1 = q1.createTailer();
                 ExcerptTailer namedTailer1 = q1.createTailer("named1");
                 ChronicleQueue q2 = SingleChronicleQueueBuilder.single(tmpDir).testBlockSize().build();
                 ExcerptTailer tailer2 = q2.createTailer();
                 ExcerptTailer namedTailer2 = q2.createTailer("named2")) {
                for (int i = 0; i < 3_000; i++) {
                    final String id0 = "" + i;
                    if (i > 0 || empty)
                        appender.writeText(id0);
                    long index0 = appender.lastIndexAppended();
//                    if ((int) index0 == 0 && i > 0)
//                        System.out.println("index: " + Long.toHexString(index0));
                    check(tailer1, id0, index0);
                    check(namedTailer1, id0, index0);
                    check(tailer2, id0, index0);
                    check(namedTailer2, id0, index0);
                    Jvm.pause(1);
                }
            }
        }
        IOTools.deleteDirWithFiles(tmpDir);
    }

    private void check(ExcerptTailer tailer, String id0, long index0) {
        try (DocumentContext dc = tailer.readingDocument()) {
            assertTrue(dc.isPresent());
            final String text = dc.wire().getValueIn().text();
            assertEquals(id0, text);
            assertEquals(Long.toHexString(index0), Long.toHexString(tailer.index()));
            assertEquals(index0, tailer.index());
        }
        final long index2 = tailer.index();
//        System.out.println("Was "+Long.toHexString(index0)+" now "+Long.toHexString(index2));
        assertEquals(index0 + 1, index2);
    }
}
