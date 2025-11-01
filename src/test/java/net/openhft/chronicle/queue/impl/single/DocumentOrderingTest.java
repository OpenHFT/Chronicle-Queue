/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.StackTrace;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.ValueOut;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.Assert.*;

public final class DocumentOrderingTest extends QueueTestCommon {
    private static final RollCycle ROLL_CYCLE = TEST_SECONDLY;
    private final ExecutorService executorService = Executors.newCachedThreadPool(
            new NamedThreadFactory("test"));
    private final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
    private final AtomicInteger counter = new AtomicInteger(0);

    private static void expectValue(final int expectedValue, final ExcerptTailer tailer) {

        try (final DocumentContext documentContext = tailer.readingDocument()) {
            assertTrue(documentContext.isPresent());
            assertEquals(expectedValue, documentContext.wire().getValueIn().int32());
        }
    }

    Thread thread;

    @Override
    @Before
    public void threadDump() {
        super.threadDump();
    }

    @Test
    public void queuedWriteInPreviousCycleShouldRespectTotalOrdering() throws InterruptedException, TimeoutException, ExecutionException {
        try (final ChronicleQueue queue =
                     builder(getTmpDir(), 1_000L).build();
             final ExcerptAppender excerptAppender = queue.createAppender()) {

            // write initial document
            excerptAppender.writeDocument("foo", ValueOut::text);

            // begin a record in the first cycle file
            final DocumentContext firstOpenDocument = excerptAppender.writingDocument();
            firstOpenDocument.wire().getValueOut().int32(counter.getAndIncrement());

            // start another record in the first cycle file
            // this may be written to either the first or the second cycle file
            final Future<RecordInfo> secondDocumentInFirstCycle = attemptToWriteDocument(queue);

            // move time to beyond the next cycle
            clock.addAndGet(TimeUnit.SECONDS.toMillis(2L));

            @SuppressWarnings("unused") final Future<RecordInfo> otherDocumentWriter = attemptToWriteDocument(queue);

            firstOpenDocument.close();
            secondDocumentInFirstCycle.get(5L, TimeUnit.SECONDS);
            final ExcerptTailer tailer = queue.createTailer();
            // discard first record
            tailer.readingDocument().close();

            otherDocumentWriter.get();

            // assert that records are committed in order
            expectValue(0, tailer);
            expectValue(1, tailer);
            expectValue(2, tailer);
            assertFalse(tailer.readingDocument().isPresent());
        }
    }

    @Test
    public void multipleThreadsMustWaitUntilPreviousCycleFileIsCompleted() throws InterruptedException, TimeoutException, ExecutionException {
        finishedNormally = false;
        final File dir = getTmpDir();
        // must be different instances of queue to work around synchronization on acquireStore()
        try (final ChronicleQueue queue =
                     builder(dir, 5_000L).build();
             final ChronicleQueue queue2 =
                     builder(dir, 5_000L).build();
             final ChronicleQueue queue3 =
                     builder(dir, 5_000L).build();
             final ChronicleQueue queue4 =
                     builder(dir, 5_000L).build();
             final ExcerptAppender excerptAppender = queue.createAppender();
        ) {

            final Future<RecordInfo> firstWriter;
            final Future<RecordInfo> secondWriter;
            final Future<RecordInfo> thirdWriter;
            try (final DocumentContext documentContext = excerptAppender.writingDocument()) {

                // move time to beyond the next cycle
                clock.addAndGet(TimeUnit.SECONDS.toMillis(2L));
                // add some jitter to allow threads to race
                firstWriter = attemptToWriteDocument(queue2);
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
                secondWriter = attemptToWriteDocument(queue3);
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
                thirdWriter = attemptToWriteDocument(queue4);

                // stall this thread, other threads should not be able to advance,
                // since this DocumentContext is still open
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2L));

                documentContext.wire().getValueOut().int32(counter.getAndIncrement());
            }

            firstWriter.get(5L, TimeUnit.SECONDS);
            secondWriter.get(5L, TimeUnit.SECONDS);
            thirdWriter.get(5L, TimeUnit.SECONDS);

            final ExcerptTailer tailer = queue.createTailer();
            expectValue(0, tailer);
            expectValue(1, tailer);
            expectValue(2, tailer);
            expectValue(3, tailer);
        }
        finishedNormally = true;
    }

    @Test
    public void shouldRecoverFromUnfinishedFirstMessageInPreviousQueue() throws InterruptedException, TimeoutException, ExecutionException {
        finishedNormally = false;
        System.setProperty("queue.force.unlock.mode", "ALWAYS");
        expectException("Couldn't acquire write lock");
        expectException("Forced unlock for the lock");
        // as below, but don't actually close the initial context
        try (final ChronicleQueue queue =
                     builder(getTmpDir(), 1_000L).build();
             final ExcerptAppender excerptAppender = queue.createAppender()) {

            final Future<RecordInfo> otherDocumentWriter;
            // begin a record in the first cycle file
            final DocumentContext documentContext = excerptAppender.writingDocument();
            documentContext.wire().getValueOut().int32(counter.getAndIncrement());

            // move time to beyond the next cycle
            clock.addAndGet(TimeUnit.SECONDS.toMillis(2L));

            otherDocumentWriter = attemptToWriteDocument(queue);

            expectCounterVaueOne(otherDocumentWriter);

            final ExcerptTailer tailer = queue.createTailer();
            expectValue(1, tailer);
            assertFalse(tailer.readingDocument().isPresent());
        } finally {
            System.clearProperty("queue.force.unlock.mode");
        }
        finishedNormally = true;
    }

    @Test
    public void codeWithinPriorDocumentMustExecuteBeforeSubsequentDocumentWhenQueueIsEmpty() throws InterruptedException, TimeoutException, ExecutionException {
        finishedNormally = false;
        try (final ChronicleQueue queue =
                     builder(getTmpDir(), 3_000L).build();
             final ExcerptAppender excerptAppender = queue.createAppender()) {

            final Future<RecordInfo> otherDocumentWriter;
            try (final DocumentContext documentContext = excerptAppender.writingDocument()) {

                // move time to beyond the next cycle
                clock.addAndGet(TimeUnit.SECONDS.toMillis(2L));

                otherDocumentWriter = attemptToWriteDocument(queue);

                // stall this thread, other thread should not be able to advance,
                // since this DocumentContext is still open
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2L));

                documentContext.wire().getValueOut().int32(counter.getAndIncrement());
            }

            expectCounterVaueOne(otherDocumentWriter);

            final ExcerptTailer tailer = queue.createTailer();
            expectValue(0, tailer);
            expectValue(1, tailer);
        }
        finishedNormally = true;
    }

    @Test
    public void codeWithinPriorDocumentMustExecuteBeforeSubsequentDocumentWhenQueueIsNotEmpty() throws InterruptedException, TimeoutException, ExecutionException {
        finishedNormally = false;
        try (final ChronicleQueue queue =
                     builder(getTmpDir(), 3_000L).build();
             final ExcerptAppender excerptAppender = queue.createAppender()) {

            excerptAppender.writeDocument("foo", ValueOut::text);
            final Future<RecordInfo> otherDocumentWriter;
            try (final DocumentContext documentContext = excerptAppender.writingDocument()) {

                // move time to beyond the next cycle
                clock.addAndGet(TimeUnit.SECONDS.toMillis(2L));

                otherDocumentWriter = attemptToWriteDocument(queue);

                // stall this thread, other thread should not be able to advance,
                // since this DocumentContext is still open
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2L));

                documentContext.wire().getValueOut().int32(counter.getAndIncrement());
            }

            expectCounterVaueOne(otherDocumentWriter);

            try (final ExcerptTailer tailer = queue.createTailer()) {
                try (final DocumentContext documentContext = tailer.readingDocument()) {
                    assertTrue(documentContext.isPresent());
                }
                expectValue(0, tailer);
                expectValue(1, tailer);
            }
        }
        finishedNormally = true;
    }

    @Override
    public void preAfter() {
        executorService.shutdownNow();
    }

    public void expectCounterVaueOne(Future<RecordInfo> otherDocumentWriter) throws InterruptedException, ExecutionException, TimeoutException {
        try {
            assertEquals(1, otherDocumentWriter.get(5L, TimeUnit.SECONDS).counterValue);
        } catch (TimeoutException e) {
            StackTrace.forThread(thread).printStackTrace();
            throw e;
        }
    }

    private Future<RecordInfo> attemptToWriteDocument(final ChronicleQueue queue) throws InterruptedException {
        final CountDownLatch startedLatch = new CountDownLatch(1);
        final Future<RecordInfo> future = executorService.submit(() -> {
            thread = Thread.currentThread();
            final int counterValue;
            startedLatch.countDown();
            try (final ExcerptAppender excerptAppender = queue.createAppender();
                 final DocumentContext documentContext = excerptAppender.writingDocument()) {
                counterValue = counter.getAndIncrement();
                documentContext.wire().getValueOut().int32(counterValue);
            }
            return new RecordInfo(counterValue);
        });
        assertTrue("Task did not start", startedLatch.await(1, TimeUnit.MINUTES));
        return future;
    }

    private SingleChronicleQueueBuilder builder(final File dir, final long timeoutMS) {
        return ChronicleQueue.singleBuilder(dir).
                testBlockSize().rollCycle(ROLL_CYCLE).
                timeProvider(clock::get).timeoutMS(timeoutMS);
    }

    private static final class RecordInfo {
        private final int counterValue;

        RecordInfo(final int counterValue) {
            this.counterValue = counterValue;
        }
    }

    @Before
    public void multiCPU() {
        Assume.assumeTrue(Runtime.getRuntime().availableProcessors() > 1);
    }
}
