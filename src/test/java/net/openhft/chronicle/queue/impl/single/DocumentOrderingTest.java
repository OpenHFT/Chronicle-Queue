package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.ValueOut;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class DocumentOrderingTest {
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
    private final AtomicInteger counter = new AtomicInteger(0);

    @Ignore("WIP")
    @Test
    public void codeWithinPriorDocumentMustExecuteBeforeSubsequentDocumentWhenQueueIsEmpty() throws InterruptedException, ExecutionException, TimeoutException {
        try (final SingleChronicleQueue queue =
                     SingleChronicleQueueBuilder.binary(DirectoryUtils.tempDir("document-ordering")).
                             testBlockSize().rollCycle(RollCycles.TEST_SECONDLY).
                             timeProvider(clock::get).build()) {

            final ExcerptAppender excerptAppender = queue.acquireAppender();
            final Future<Boolean> otherDocumentWriter;
            try (final DocumentContext documentContext = excerptAppender.writingDocument()) {

                // move time to beyond the next cycle
                clock.addAndGet(TimeUnit.SECONDS.toMillis(2L));

                otherDocumentWriter = executorService.submit(attemptToWriteDocument(queue));

                // stall this thread, other thread should not be able to advance,
                // since this DocumentContext is still open
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2L));

                documentContext.wire().getValueOut().int32(counter.getAndIncrement());
            }

            assertTrue(otherDocumentWriter.get(5L, TimeUnit.SECONDS));

            final ExcerptTailer tailer = queue.createTailer();
            expectValue(0, tailer);
            expectValue(1, tailer);
        }
    }

    @Test
    public void codeWithinPriorDocumentMustExecuteBeforeSubsequentDocumentWhenQueueIsNotEmpty() throws InterruptedException, ExecutionException, TimeoutException {
        try (final SingleChronicleQueue queue =
                     SingleChronicleQueueBuilder.binary(DirectoryUtils.tempDir("document-ordering")).
                             testBlockSize().rollCycle(RollCycles.TEST_SECONDLY).
                             timeProvider(clock::get).timeoutMS(5_000L).build()) {

            final ExcerptAppender excerptAppender = queue.acquireAppender();
            excerptAppender.writeDocument("foo", ValueOut::text);
            final Future<Boolean> otherDocumentWriter;
            try (final DocumentContext documentContext = excerptAppender.writingDocument()) {

                // move time to beyond the next cycle
                clock.addAndGet(TimeUnit.SECONDS.toMillis(2L));

                otherDocumentWriter = executorService.submit(attemptToWriteDocument(queue));

                // stall this thread, other thread should not be able to advance,
                // since this DocumentContext is still open
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2L));

                documentContext.wire().getValueOut().int32(counter.getAndIncrement());
            }

            assertTrue(otherDocumentWriter.get(5L, TimeUnit.SECONDS));

            final ExcerptTailer tailer = queue.createTailer();
            final DocumentContext documentContext = tailer.readingDocument();
            assertTrue(documentContext.isPresent());
            documentContext.close();
            expectValue(0, tailer);
            expectValue(1, tailer);
        }
    }

    private static void expectValue(final int expectedValue, final ExcerptTailer tailer) {
        try( final DocumentContext documentContext = tailer.readingDocument()) {
            assertTrue(documentContext.isPresent());
            assertEquals(expectedValue, documentContext.wire().getValueIn().int32());
        }
    }

    private Callable<Boolean> attemptToWriteDocument(final SingleChronicleQueue queue) {
        return () -> {
            try (final DocumentContext documentContext = queue.acquireAppender().writingDocument()) {
                documentContext.wire().getValueOut().int32(counter.getAndIncrement());
            }
            return true;
        };
    }
}