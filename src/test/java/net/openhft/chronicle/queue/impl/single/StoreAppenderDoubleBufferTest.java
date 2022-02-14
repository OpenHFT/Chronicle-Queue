package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.*;

import static net.openhft.chronicle.queue.DirectoryUtils.tempDir;
import static net.openhft.chronicle.queue.RollCycles.MINUTELY;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class StoreAppenderDoubleBufferTest extends QueueTestCommon {

    private static final Logger LOGGER = LoggerFactory.getLogger(StoreAppenderDoubleBufferTest.class);

    private final int iterations;
    private final Class<? extends BlockedWriterScenario> scenarioClass;

    public StoreAppenderDoubleBufferTest(int iterations, Class<? extends BlockedWriterScenario> scenarioClass) {
        this.iterations = iterations;
        this.scenarioClass = scenarioClass;
    }

    @Parameterized.Parameters(name = "iterations={0},scenarioClass={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{1, RawBlockedWriterScenario.class},
                new Object[]{2, RawBlockedWriterScenario.class},
                new Object[]{3, RawBlockedWriterScenario.class},
                new Object[]{1, MethodWriterBlockedWriterScenario.class},
                new Object[]{2, MethodWriterBlockedWriterScenario.class},
                new Object[]{3, MethodWriterBlockedWriterScenario.class},
                new Object[]{5, RollbackBlockedWriterScenario.class},
                new Object[]{1, CallIndexWhileBlockedWriterScenario.class}
        );
    }

    @Test
    public void disabled() {}

//    @Test(timeout = 10000L)
    public void testDoubleBuffering() throws InterruptedException, ExecutionException, NoSuchMethodException,
            IllegalAccessException, InvocationTargetException, InstantiationException {
        try (SingleChronicleQueue q = binary(tempDir("q"))
                .doubleBuffer(true)
                .rollCycle(MINUTELY)
                .timeProvider(() -> 0).build()) {

            BlockedWriterScenario blockedWriterScenario = scenarioClass.getConstructor(ChronicleQueue.class, Integer.class)
                    .newInstance(q, iterations);
            blockedWriterScenario.run();

            ExcerptTailer tailer = q.createTailer();

            for (int i = 0; i < iterations; i++) {
                try (DocumentContext dc = tailer.readingDocument()) {
                    assertEquals("blocker-before", dc.wire().read().text());
                    assertEquals("blocker-after", dc.wire().read().text());
                }
                blockedWriterScenario.readBlockeeRecordForIteration(i, tailer);
            }
            // Test we're at the end
            try (DocumentContext dc = tailer.readingDocument()) {
                assertFalse(dc.isPresent());
            }
        }
    }

    /**
     * Run two threads, the first thread creates a DocumentContext then
     * blocks with it open, forcing the second thread to double-buffer.
     * <p>
     * The blocked thread notifies the blocking thread when it's acquired
     * its (double-buffered) DocumentContext to allow the blocker to
     * release its DocumentContext.
     */
    private abstract static class BlockedWriterScenario {

        protected final ChronicleQueue queue;
        protected final CyclicBarrier everyoneHasAppenders =
                new CyclicBarrier(2, () -> LOGGER.info("Everyone has appenders"));
        protected final CyclicBarrier blockerHasDocumentContext =
                new CyclicBarrier(2, () -> LOGGER.info("Blocker has DC"));
        protected final CyclicBarrier blockeeHasDocumentContext =
                new CyclicBarrier(2, () -> LOGGER.info("Blockee has DC"));
        protected final CyclicBarrier iterationFinished =
                new CyclicBarrier(2, () -> LOGGER.info("Iteration finished"));
        protected final int iterations;

        public BlockedWriterScenario(ChronicleQueue queue, Integer iterations) {
            this.queue = queue;
            this.iterations = iterations;
        }

        public void run() throws ExecutionException, InterruptedException {
            ExecutorService executorService = Executors.newFixedThreadPool(2);
            try {
                Future<?> blockerFuture = executorService.submit(this::runBlocker);
                Future<?> blockedFuture = executorService.submit(this::runBlockee);
                blockedFuture.get();
                blockerFuture.get();
            } finally {
                executorService.shutdown();
            }
        }

        public void runBlocker() {
            LOGGER.info("--- Starting {} iterations ({}) --", iterations, this.getClass().getSimpleName());
            try (ExcerptAppender appender = queue.acquireAppender()) {
                everyoneHasAppenders.await();
                for (int i = 0; i < iterations; i++) {
                    LOGGER.info("--- Starting iteration {}/{} --", i + 1, iterations);
                    try (DocumentContext ctx = appender.writingDocument()) {
                        blockerHasDocumentContext.await();
                        ctx.wire().write().text("blocker-before");
                        blockeeHasDocumentContext.await();
                        ctx.wire().write().text("blocker-after");
                    }
                    iterationFinished.await();
                }
                LOGGER.info("Blocker finished");
            } catch (InterruptedException | BrokenBarrierException ex) {
                fail();
            }
        }

        public void runBlockee() {
            try (ExcerptAppender appender = queue.acquireAppender()) {
                everyoneHasAppenders.await();
                for (int i = 0; i < iterations; i++) {
                    blockerHasDocumentContext.await();
                    writeBlockeeRecordForIteration(i, appender);
                    iterationFinished.await();
                }
                LOGGER.info("Blockee finished");
            } catch (InterruptedException | BrokenBarrierException e) {
                fail();
            }
        }

        /**
         * Implementors must call blockeeHasDocumentContext.await() when they have a document context
         */
        abstract protected void writeBlockeeRecordForIteration(int iteration, ExcerptAppender appender)
                throws InterruptedException, BrokenBarrierException;

        abstract public void readBlockeeRecordForIteration(int iteration, ExcerptTailer tailer);
    }

    /**
     * The blocked writer uses a chaining methodWriter with history enabled
     */
    static class MethodWriterBlockedWriterScenario extends BlockedWriterScenario {

        public MethodWriterBlockedWriterScenario(ChronicleQueue queue, Integer iterations) {
            super(queue, iterations);
        }

        @Override
        protected void writeBlockeeRecordForIteration(int iteration, ExcerptAppender appender) {
            Foo foo = appender.methodWriterBuilder(Foo.class)
                    .updateInterceptor((name, arg) -> {
                        if ("foo".equals(name)) {
                            try {
                                blockeeHasDocumentContext.await();
                            } catch (InterruptedException | BrokenBarrierException e) {
                                fail();
                            }
                        }
                        return true;
                    })
                    .build();
            foo.foo("foo").bar("bar");
        }

        @Override
        public void readBlockeeRecordForIteration(int iteration, ExcerptTailer tailer) {
            CountingFoo countingFoo = new CountingFoo();
            try (MethodReader methodReader = tailer.methodReaderBuilder().build(countingFoo)) {
                assertTrue(methodReader.readOne());
                assertEquals(1, countingFoo.fooCount);
                assertEquals(1, countingFoo.barCount);
            }
        }

        interface Foo {
            Bar foo(String str);
        }

        interface Bar {
            void bar(String str);
        }

        static class CountingFoo implements Foo {

            int fooCount = 0;
            int barCount = 0;

            @Override
            public Bar foo(String str) {
                fooCount++;
                return msg -> barCount++;
            }
        }
    }

    /**
     * The blocked writer writes directly to the document context
     */
    static class RawBlockedWriterScenario extends BlockedWriterScenario {

        public RawBlockedWriterScenario(ChronicleQueue queue, Integer iterations) {
            super(queue, iterations);
        }

        @Override
        protected void writeBlockeeRecordForIteration(int iteration, ExcerptAppender appender)
                throws BrokenBarrierException, InterruptedException {
            try (DocumentContext dc = appender.writingDocument()) {
                blockeeHasDocumentContext.await();
                writeRecordContents(iteration, dc);
            }
        }

        protected void writeRecordContents(int iteration, DocumentContext documentContext) {
            documentContext.wire().write().text("blocked!");
        }

        @Override
        public void readBlockeeRecordForIteration(int iteration, ExcerptTailer tailer) {
            try (DocumentContext dc = tailer.readingDocument()) {
                assertEquals("blocked!", dc.wire().read().text());
            }
        }
    }

    /**
     * The blocked writer rolls back every second write, to test double-buffered
     * rollback and its impact on subsequent writes.
     */
    static class RollbackBlockedWriterScenario extends RawBlockedWriterScenario {

        public RollbackBlockedWriterScenario(ChronicleQueue queue, Integer iterations) {
            super(queue, iterations);
            if (iterations < 3) {
                throw new IllegalArgumentException("This test is only meaningful with at least three iterations");
            }
        }

        @Override
        protected void writeRecordContents(int iteration, DocumentContext documentContext) {
            super.writeRecordContents(iteration, documentContext);
            if (shouldRollBack(iteration)) {
                documentContext.rollbackOnClose();
            }
        }

        @Override
        public void readBlockeeRecordForIteration(int iteration, ExcerptTailer tailer) {
            if (shouldRollBack(iteration)) {
                // Nothing should have been written for this iteration
                return;
            }
            super.readBlockeeRecordForIteration(iteration, tailer);
        }

        private boolean shouldRollBack(int iteration) {
            return iteration % 2 == 0;
        }
    }

    /**
     * The blocked writer calls DocumentContext.index() to ensure it raises IndexNotAvailableException
     */
    static class CallIndexWhileBlockedWriterScenario extends RawBlockedWriterScenario {

        public CallIndexWhileBlockedWriterScenario(ChronicleQueue queue, Integer iterations) {
            super(queue, iterations);
        }

        @Override
        protected void writeRecordContents(int iteration, DocumentContext documentContext) {
            try {
                documentContext.index();
                fail();
            } catch (IndexNotAvailableException e) {
                // Expect index to not be available when double buffered
                super.writeRecordContents(iteration, documentContext);
            }
        }
    }
}