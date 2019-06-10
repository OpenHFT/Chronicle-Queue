package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.MethodReader;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static net.openhft.chronicle.queue.RollCycles.TEST_DAILY;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ChronicleQueueMethodsWithoutParameters extends ChronicleQueueTestBase {

    protected static final Logger LOG = LoggerFactory.getLogger(ChronicleQueueMethodsWithoutParameters.class);

    @Test
    public void test() {
        File file = getTmpDir();

        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(file)
                .testBlockSize()
                .rollCycle(TEST_DAILY).build()) {

            SomeListener someListener = queue.acquireAppender().methodWriter(SomeListener.class);

            SomeManager someManager = new SomeManager();
            MethodReader reader = queue.createTailer().methodReader(someManager);

            LOG.debug("Writing to queue");
            someListener.methodWithOneParam(1);
            someListener.methodWithoutParams();

            LOG.debug("Reading from queue");
            assertTrue(reader.readOne());
            assertTrue(reader.readOne());
            assertFalse(reader.readOne());

            assertTrue(someManager.methodWithOneParamInvoked);       // one param method was invoked
            assertTrue(someManager.methodWithoutParamsInvoked);      // no params method was NOT invoked

            LOG.warn(queue.dump());
        }
    }

    public interface SomeListener {

        void methodWithoutParams();

        void methodWithOneParam(int i);
    }

    public static class SomeManager implements SomeListener {

        public boolean methodWithoutParamsInvoked = false;
        public boolean methodWithOneParamInvoked = false;

        @Override
        public void methodWithoutParams() {
            methodWithoutParamsInvoked = true;
        }

        @Override
        public void methodWithOneParam(int i) {
            methodWithOneParamInvoked = true;
        }
    }
}
