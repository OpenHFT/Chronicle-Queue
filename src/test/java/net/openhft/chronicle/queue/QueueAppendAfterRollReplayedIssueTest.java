package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

/*
    Scenario:
    Queue is reloaded with roll files present and
    writingDocument retrieved with acquireDocument
 */
public class QueueAppendAfterRollReplayedIssueTest extends QueueTestCommon {

    @Test
    public void test() {
        int messages = 10;

        String path = OS.getTarget() + "/" + getClass().getSimpleName() + "-" + Time.uniqueId();
        SetTimeProvider timeProvider = new SetTimeProvider();
        try (final ChronicleQueue writeQueue = ChronicleQueue
                .singleBuilder(path)
                .testBlockSize()
                .timeProvider(timeProvider)
                .rollCycle(RollCycles.TEST_SECONDLY).build()) {
            for (int i = 0; i < messages; i++) {
                timeProvider.advanceMillis(i * 100);
                ExcerptAppender appender = writeQueue.acquireAppender();
                Map<String, Object> map = new HashMap<>();
                map.put("key", i);
                appender.writeMap(map);
            }
        }

        timeProvider.advanceMillis(1000);

        try (final ChronicleQueue queue = ChronicleQueue
                .singleBuilder(path)
                .testBlockSize()
                .timeProvider(timeProvider)
                .rollCycle(RollCycles.TEST_SECONDLY).build()) {
            final ExcerptAppender excerptAppender = queue.acquireAppender();
            try(final DocumentContext documentContext = excerptAppender.acquireWritingDocument(false)){
                assertNotNull(documentContext.wire());
            }

        } finally {
            try {
                IOTools.deleteDirWithFiles(path, 2);
            } catch (IORuntimeException todoFixOnWindows) {

            }
        }
    }
}