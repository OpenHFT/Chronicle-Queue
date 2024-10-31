package net.openhft.chronicle.queue.issue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DeleteFileTest {
    @Test
    public void testMain() {
        final long[] clock = {1730366325_000L};
        final long delay = 1_000L;
        try {
            try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary("queue")
                    .timeProvider(() -> clock[0])
                    .testBlockSize()
                    .rollCycle(TestRollCycles.TEST4_SECONDLY)
                    .forceDirectoryListingRefreshIntervalMs(delay)
                    .build();
                 ExcerptAppender appender = queue.createAppender();
                 ExcerptTailer tailerF0 = queue.createTailer();
                 ExcerptTailer tailerA = queue.createTailer()) {

                // a tailer that is closed and re-opened on windows and reused on Linux.
                ExcerptTailer tailer0 = tailerF0;

                appender.writeText("1");

                assertEquals("1", tailer0.readText());
                final File firstCycleFile = appender.currentFile();
                clock[0] += queue.rollCycle().lengthInMillis();
                appender.writeText("2");

//                System.out.println(queue.dump());

//                queue.refreshDirectoryListing();

                // can't delete while in use on windows, so have to close.
                if (!OS.isLinux())
                    tailer0.close();
                BackgroundResourceReleaser.releasePendingResources();
                assertTrue(firstCycleFile.delete());

//                System.out.println(queue.dump());

                if (!OS.isLinux())
                    tailer0 = queue.createTailer();

                // before this delay, the tailer is still using the cached directory listing.
                clock[0] += delay;

                // a tailer created at the start but not used until after the file was deleted.
                String twoA = tailerA.readText();

                String two0 = tailer0.readText();

                // a tailer created after the file was deleted.
                String twoB;
                try (ExcerptTailer tailer = queue.createTailer()) {
                    twoB = tailer.readText();
                }

                // a named tailer created after the file was deleted.
                String twoC;
                try (ExcerptTailer tailer = queue.createTailer("tailer")) {
                    twoC = tailer.readText();
                }

                assertEquals("2 2 2 2", twoA + " " + two0 + " " + twoB + " " + twoC);
            }
        } finally {
            BackgroundResourceReleaser.releasePendingResources();
            IOTools.deleteDirWithFilesOrThrow("queue");
        }
    }
}
