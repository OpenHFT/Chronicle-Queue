/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.issue;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression test for OpenHFT/Chronicle-Queue#1703 (Jira: QUEUE-118).
 * <p>
 * Before the fix, when a queue was built with {@code readOnly(false)} but
 * {@link net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder#initializeMetadata()}
 * could not open the metadata file for writing, it silently fell back to a
 * {@link net.openhft.chronicle.queue.impl.table.ReadonlyTableStore}, the constructor
 * force-flipped {@code readOnly} to {@code true}, and a later
 * {@code createTailer(String)} surfaced as
 * {@code UnsupportedOperationException: "Read only"} from
 * {@code ReadonlyTableStore.doWithExclusiveLock}. That exception message was misleading
 * because the caller explicitly asked for a writable queue.
 * <p>
 * The silent fallback itself is retained - see
 * {@code ReadWriteTest.testNonWriteableFilesSetToReadOnly} - because unnamed tailers can
 * still read usefully from a degraded queue. The fix is scoped to named tailers: when
 * {@code createTailer(String)} is invoked on a read-only queue, fail fast with an
 * {@link IllegalStateException} that explains the root cause instead of letting the
 * caller see a raw {@code UnsupportedOperationException}.
 * <p>
 * The test forces the writable metadata open to fail by setting
 * {@code metadata.cq4t} not-writable on disk. Windows handles read-only mapping
 * differently and is excluded.
 */
@DisabledOnOs(OS.WINDOWS)
public class NamedTailerReadOnlyFallbackTest {

    @Test
    public void createNamedTailerFailsFastWhenQueueFellBackToReadonly(@TempDir Path tempDir) {
        final File queueDir = tempDir.resolve("q").toFile();

        // 1. Create the queue once with readOnly(false) so metadata.cq4t exists on disk.
        try (ChronicleQueue creation = SingleChronicleQueueBuilder.binary(queueDir)
                .readOnly(false)
                .build()) {
            assertNotNull(creation);
        }

        File metadata = new File(queueDir, "metadata.cq4t");
        assertTrue(metadata.exists(), "metadata.cq4t should have been created by the first open");
        assertTrue(metadata.setWritable(false, false),
                "pre-condition for the test: FS must honour setWritable(false)");

        try {
            // 2. Re-open with readOnly(false). initializeMetadata() will catch the write
            //    failure, log "Failback to readonly tablestore", install a ReadonlyTableStore,
            //    and the constructor will force-flip readOnly to true. This intentional
            //    graceful-degrade is covered by ReadWriteTest.testNonWriteableFilesSetToReadOnly
            //    and is not changed by this fix.
            try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDir)
                    .readOnly(false)
                    .build()) {

                // 3. An unnamed tailer must still work. The graceful-degrade contract means
                //    read-side consumers keep functioning even when the metadata file is lost.
                try (ExcerptTailer unnamed = queue.createTailer()) {
                    assertNotNull(unnamed, "unnamed createTailer() must still work on a degraded queue");
                }

                // 4. A named tailer, however, requires writable metadata. Before the fix this
                //    threw UnsupportedOperationException("Read only") from deep inside
                //    ReadonlyTableStore, which was misleading. After the fix it must throw
                //    IllegalStateException with a message that names the queue path and points
                //    at the Failback warning.
                IllegalStateException ex = assertThrows(IllegalStateException.class,
                        () -> queue.createTailer("repro-named"),
                        "createTailer(String) on a read-only queue must fail fast with a clear message");

                String msg = String.valueOf(ex.getMessage());
                assertTrue(msg.contains("repro-named"),
                        "message should include the tailer id; was: " + msg);
                assertTrue(msg.contains("read-only"),
                        "message should explain that the queue is read-only; was: " + msg);
                assertTrue(msg.contains(queueDir.getAbsolutePath()) || msg.contains(queueDir.getName()),
                        "message should reference the queue path; was: " + msg);

                // 5. Guard against regression: the old misleading UOE must no longer leak out.
                //    If anyone later re-routes the failure through ReadonlyTableStore, this
                //    assertion will catch it.
                for (Throwable t = ex; t != null; t = t.getCause()) {
                    if (t instanceof UnsupportedOperationException
                            && "Read only".equals(t.getMessage())) {
                        fail("Regression: the raw UnsupportedOperationException(\"Read only\") "
                                + "from ReadonlyTableStore is being surfaced to callers of "
                                + "createTailer(String). It should be replaced by IllegalStateException "
                                + "with a descriptive message before it ever reaches here.");
                    }
                }
            }
        } finally {
            //noinspection ResultOfMethodCallIgnored
            metadata.setWritable(true, false);
        }
    }
}
