/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.onoes.ExceptionKey;
import net.openhft.chronicle.wire.Sequence;
import org.junit.jupiter.api.Test;

import java.io.StreamCorruptedException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Drives the {@code sequenceForPosition(MAX_VALUE)} fast path's retry loop to completion by
 * injecting a {@link Sequence} that always returns {@link Sequence#NOT_FOUND_RETRY}. Verifies
 * that:
 * <ol>
 *   <li>the loop spins up to the configured retry budget, then logs a {@code retry loop
 *       exhausted} warning;
 *   <li>the call falls through cleanly to the original indexed-lookup path and still produces
 *       the correct sequence -- i.e. exhaustion is recoverable, not an error.
 * </ol>
 *
 * <p>The test temporarily shrinks {@link SCQIndexing#SEQUENCE_TRACKER_RETRY_BUDGET} so the loop
 * exits in a handful of iterations rather than the production-default 128: each retry invokes
 * the production-side {@code Thread.yield()} (after retry &gt; 2), which would otherwise mean
 * &gt;120 OS scheduler hits per test run.
 *
 * <p>Injection is via direct write to the package-private {@link SCQIndexing#sequence} field;
 * this is why the test lives in the {@code single} package.
 */
class SequenceForPositionRetryExhaustionTest extends IndexingTestCommon {

    private static final int TEST_RETRY_BUDGET = 8;

    @Test
    void retryExhaustionLogsAndFallsThroughToIndexedLookup() throws StreamCorruptedException {
        // One write is enough -- we only need a non-empty queue so the indexed-lookup
        // fall-through has something to scan.
        appender.writeText("entry");

        SingleChronicleQueueStore store = appender.store;
        SCQIndexing indexing = store.indexing;
        long expectedSeq = queue.rollCycle().toSequenceNumber(appender.lastIndexAppended());

        int originalBudget = SCQIndexing.SEQUENCE_TRACKER_RETRY_BUDGET;
        SCQIndexing.SEQUENCE_TRACKER_RETRY_BUDGET = TEST_RETRY_BUDGET;
        Sequence original = indexing.sequence;
        AlwaysRetrySequence stuck = new AlwaysRetrySequence();
        indexing.sequence = stuck;

        Map<ExceptionKey, Integer> recorded = Jvm.recordExceptions();
        try {
            long observed = indexing.sequenceForPosition(appender, Long.MAX_VALUE, false);
            assertEquals(expectedSeq, observed,
                    "After retry exhaustion, the indexed-lookup fall-through must still produce the latest sequence");
        } finally {
            indexing.sequence = original;
            SCQIndexing.SEQUENCE_TRACKER_RETRY_BUDGET = originalBudget;
            Jvm.resetExceptionHandlers();
        }

        assertTrue(stuck.calls.get() >= TEST_RETRY_BUDGET,
                "The fast path should have exercised the full retry budget; observed " +
                        stuck.calls.get() + " calls");

        boolean exhaustionLogged = recorded.keySet().stream()
                .anyMatch(k -> k.message() != null && k.message().contains("tracker-read retry loop exhausted"));
        assertTrue(exhaustionLogged,
                "Expected a 'tracker-read retry loop exhausted' log entry; observed messages: " + recorded.keySet());
    }

    /**
     * Minimal {@link Sequence} stub that always reports {@link Sequence#NOT_FOUND_RETRY},
     * counting invocations so the test can assert the retry budget was fully exercised.
     */
    private static final class AlwaysRetrySequence implements Sequence {
        final AtomicInteger calls = new AtomicInteger();

        @Override
        public long getSequence(long forWritePosition) {
            calls.incrementAndGet();
            return Sequence.NOT_FOUND_RETRY;
        }

        @Override
        public void setSequence(long sequence, long position) {
            // not used in this test
        }

        @Override
        public long toIndex(long headerNumber, long sequence) {
            return 0;
        }

        @Override
        public long toSequenceNumber(long index) {
            return 0;
        }
    }
}
