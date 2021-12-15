package net.openhft.chronicle.queue;

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
                new Object[]{true},
                new Object[]{false}
        );
    }

    @Test
    public void multipleTailers() {
        File tmpDir = new File(OS.getTarget(), "multipleTailers" + System.nanoTime());

        try (ChronicleQueue q1 = SingleChronicleQueueBuilder.single(tmpDir).testBlockSize()
//                .timeProvider(new SetTimeProvider("2021/12/15T11:22:33").advanceMicros(10))
                .rollCycle(RollCycles.TEST_SECONDLY).build();
             ExcerptAppender appender = q1.acquireAppender();
             ExcerptTailer tailer1 = q1.createTailer();
             ExcerptTailer namedTailer1 = q1.createTailer("named1");
             ChronicleQueue q2 = SingleChronicleQueueBuilder.single(tmpDir).testBlockSize().build();
             ExcerptTailer tailer2 = q2.createTailer();
             ExcerptTailer namedTailer2 = q2.createTailer("named2")) {
            if (!empty)
                appender.writeText("0");
            for (int i = 0; i < 1_000_000; i++) {
                final String id0 = "" + (i + (empty ? 0 : 1));
                appender.writeText(id0);
                final long index0 = appender.lastIndexAppended();
                check(tailer1, id0, index0);
                check(namedTailer1, id0, index0);
                check(tailer2, id0, index0);
                check(namedTailer2, id0, index0);
            }
        }
        IOTools.deleteDirWithFiles(tmpDir);
    }

    private void check(ExcerptTailer tailer1, String id0, long index0) {
        try (DocumentContext dc = tailer1.readingDocument()) {
            assertTrue(dc.isPresent());
            assertEquals(Long.toHexString(index0), Long.toHexString(tailer1.index()));
            assertEquals(index0, tailer1.index());
            assertEquals(id0, dc.wire().getValueIn().text());
        }
    }
}
