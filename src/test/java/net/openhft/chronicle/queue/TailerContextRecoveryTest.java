/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.openhft.chronicle.queue.TailerContextRecovery.Status.NO_NAMED_POSITION;
import static net.openhft.chronicle.queue.TailerContextRecovery.Status.REPLAYED_TO_NAMED_INDEX;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.Assert.*;

public class TailerContextRecoveryTest extends QueueTestCommon {

    @Test
    public void restartedNamedTailerRebuildsCurrentCycleContextBeforeContinuing() {
        File path = getTmpDir();
        SetTimeProvider timeProvider = new SetTimeProvider(1_000_000_000L);

        long resumeIndex;
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(path)
                .rollCycle(TEST_SECONDLY)
                .timeProvider(timeProvider)
                .contextListener(MarketContext.class, context -> context.instrument(17, "EUR/USD"))
                .build()) {
            MarketEvents writer = queue.createAppender().methodWriter(MarketEvents.class);
            writer.trade(17, 10);
            writer.trade(17, 20);

            MarketProjection firstRunProjection = new MarketProjection();
            MarketEventHandler firstRunHandler = new MarketEventHandler(firstRunProjection);
            try (ExcerptTailer namedTailer = queue.createTailer("risk-engine")) {
                MethodReader reader = namedTailer.methodReader(firstRunHandler);
                assertTrue(reader.readOne());
                assertTrue(reader.readOne());
                resumeIndex = namedTailer.index();
            }
            assertEquals(Collections.singletonList("EUR/USD:10"), firstRunHandler.trades);

            MarketProjection restartedProjection = new MarketProjection();
            try (ExcerptTailer restartedTailer = queue.createTailer("risk-engine")) {
                assertEquals(resumeIndex, restartedTailer.index());

                TailerContextRecovery.ReplayResult result =
                        TailerContextRecovery.replayCurrentCycleContext(restartedTailer, restartedProjection);

                assertEquals(REPLAYED_TO_NAMED_INDEX, result.status());
                assertTrue(result.complete());
                assertEquals(2, result.documentsScanned());
                assertEquals("EUR/USD", restartedProjection.symbolName(17));
                assertEquals("context replay must not advance the named tailer",
                        resumeIndex, restartedTailer.index());

                MarketEventHandler restartedHandler = new MarketEventHandler(restartedProjection);
                MethodReader reader = restartedTailer.methodReader(restartedHandler);
                assertTrue(reader.readOne());
                assertEquals(Collections.singletonList("EUR/USD:20"), restartedHandler.trades);
                assertFalse(reader.readOne());
            }
        }
    }

    @Test
    public void freshNamedTailerHasNoContextToReplay() {
        File path = getTmpDir();

        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(path)
                .rollCycle(TEST_SECONDLY)
                .build()) {
            try (ExcerptTailer namedTailer = queue.createTailer("fresh")) {
                TailerContextRecovery.ReplayResult result =
                        TailerContextRecovery.replayCurrentCycleContext(namedTailer, new MarketProjection());

                assertEquals(NO_NAMED_POSITION, result.status());
                assertTrue(result.complete());
                assertEquals(0, result.documentsScanned());
                assertEquals(0, namedTailer.index());
            }
        }
    }

    public interface MarketContext {
        void instrument(int symbolId, String name);
    }

    public interface MarketEvents extends MarketContext {
        void trade(int symbolId, long quantity);
    }

    public static final class MarketProjection implements MarketContext {
        private final Map<Integer, String> symbolNames = new HashMap<>();

        @Override
        public void instrument(int symbolId, String name) {
            symbolNames.put(symbolId, name);
        }

        private String symbolName(int symbolId) {
            return symbolNames.get(symbolId);
        }
    }

    public static final class MarketEventHandler implements MarketEvents {
        private final MarketProjection projection;
        private final List<String> trades = new ArrayList<>();

        private MarketEventHandler(MarketProjection projection) {
            this.projection = projection;
        }

        @Override
        public void instrument(int symbolId, String name) {
            projection.instrument(symbolId, name);
        }

        @Override
        public void trade(int symbolId, long quantity) {
            String symbolName = projection.symbolName(symbolId);
            if (symbolName == null)
                throw new IllegalStateException("Missing instrument context for " + symbolId);
            trades.add(symbolName + ":" + quantity);
        }
    }
}
