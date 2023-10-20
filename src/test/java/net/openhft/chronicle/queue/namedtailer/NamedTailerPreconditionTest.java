package net.openhft.chronicle.queue.namedtailer;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.NamedTailerNotAvailableException;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;


public class NamedTailerPreconditionTest extends QueueTestCommon {

    /**
     * Simulates append lock being held which would happen during replication on a sink. It should not be possible
     * to instantiate the named tailer in this case.
     */
    @Test
    public void cannotCreateNamedTailerIfAppendLockIsHeld() {
        File queuePath = getTmpDir();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(queuePath).build()) {
            queue.appendLock().lock();
            NamedTailerNotAvailableException exception = assertThrows(
                    "Named tailer cannot be created because: Named tailers cannot be instantiated on a replication sink",
                    NamedTailerNotAvailableException.class,
                    () -> queue.createTailer("named_1")
            );
            assertEquals("named_1", exception.tailerName());
            assertEquals(NamedTailerNotAvailableException.Reason.NOT_AVAILABLE_ON_SINK, exception.reason());
        } finally {
            IOTools.deleteDirWithFiles(queuePath);
        }
    }

}
