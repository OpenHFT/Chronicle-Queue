package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.AbstractReferenceCounted;
import net.openhft.chronicle.core.onoes.ExceptionKey;
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.queue.impl.single.QueueFileShrinkManager;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.StoreComponentReferenceHandler;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.util.Map;

public class QueueTestCommon {
    private ThreadDump threadDump;
    private Map<ExceptionKey, Integer> exceptions;

    @Before
    public void enableReferenceTracing() {
        AbstractReferenceCounted.enableReferenceTracing();
    }

    public void assertReferencesReleased() {
        AbstractReferenceCounted.assertReferencesReleased();
    }

    @Before
    public void threadDump() {
        threadDump = new ThreadDump();
    }

    public void checkThreadDump() {
        threadDump.ignore(StoreComponentReferenceHandler.THREAD_NAME);
        threadDump.ignore(SingleChronicleQueue.DISK_SPACE_CHECKER_NAME);
        threadDump.ignore(QueueFileShrinkManager.THREAD_NAME);
        threadDump.assertNoNewThreads();
    }

    @Before
    public void recordExceptions() {
        exceptions = Jvm.recordExceptions();
    }

    public void checkExceptions() {
        if (Jvm.hasException(exceptions)) {
            Jvm.dumpException(exceptions);
            Jvm.resetExceptionHandlers();
            Assert.fail();
        }
    }

    @After
    public void afterChecks() {
        try {
            assertReferencesReleased();
            checkThreadDump();
            checkExceptions();
        } catch (Throwable e) {
            System.err.println(e);
        }
    }
}
