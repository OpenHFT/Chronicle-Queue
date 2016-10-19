package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.MethodReader;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Created on 19.10.2016.
 */
public class ChronicleQueueMethodsWithoutParameters extends ChronicleQueueTestBase {

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

    protected static final Logger LOG = LoggerFactory.getLogger(ChronicleQueueMethodsWithoutParameters.class);

    @Test
    public void test() throws IOException, InterruptedException {
        File file = getTmpDir();

        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(file).build()) {
            final ExcerptAppender appender = queue.acquireAppender();
            final ExcerptTailer tailer = queue.createTailer();

            SomeListener someListener = appender.methodWriter(SomeListener.class);

            SomeManager someManager = new SomeManager();
            MethodReader reader = tailer.methodReader(someManager);

            LOG.debug("Writing to queue");
            someListener.methodWithOneParam(1);
            someListener.methodWithoutParams();

            LOG.debug("Reading from queue");
            boolean run = true;
            while (run) {
                run = reader.readOne();
            }

            Assert.assertEquals(true, someManager.methodWithOneParamInvoked);
            Assert.assertEquals(true, someManager.methodWithoutParamsInvoked);

            LOG.warn(queue.dump());
        }
    }
}
