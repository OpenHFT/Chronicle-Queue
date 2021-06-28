package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import org.junit.Test;

import java.io.File;

import static net.openhft.chronicle.queue.RollCycles.TEST_DAILY;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ChronicleQueueMethodsWithoutParametersTest extends ChronicleQueueTestBase {

    @Test
    public void test() {
        File file = getTmpDir();

        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(file)
                .testBlockSize()
                .rollCycle(TEST_DAILY).build()) {

            SomeListener someListener = queue.acquireAppender()
                    .methodWriter(SomeListener.class);

            SomeManager someManager = new SomeManager();
            MethodReader reader = queue.createTailer()
                    .methodReader(someManager);

            Jvm.debug().on(getClass(), "Writing to queue");
            someListener.methodWithOneParam(1);
            someListener.methodWithoutParams();

            Jvm.debug().on(getClass(), "Reading from queue");
            assertTrue(reader.readOne());
            assertTrue(reader.readOne());
            assertFalse(reader.readOne());

            assertTrue(someManager.methodWithOneParamInvoked);       // one param method was invoked
            assertTrue(someManager.methodWithoutParamsInvoked);      // no params method was NOT invoked

           // Jvm.warn().on(getClass(), queue.dump());
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
