package net.openhft.chronicle.queue.incubator.streaming.demo.accumulation;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.AppenderListener;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.incubator.streaming.Accumulation;
import net.openhft.chronicle.queue.incubator.streaming.Accumulations;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.*;
import static java.util.stream.Collectors.toConcurrentMap;
import static net.openhft.chronicle.queue.incubator.streaming.CollectorUtil.replacingMerger;
import static net.openhft.chronicle.queue.incubator.streaming.CollectorUtil.toConcurrentSet;
import static net.openhft.chronicle.queue.incubator.streaming.ExcerptExtractor.builder;
import static org.junit.Assert.assertEquals;

public class LastMarketDataPerSymbolTest extends ChronicleQueueTestBase {

    private static final String Q_NAME = LastMarketDataPerSymbolTest.class.getSimpleName();

    private static final List<MarketData> MARKET_DATA_SET = Arrays.asList(
            new MarketData("MSFT", 100, 110, 90),
            new MarketData("APPL", 200, 220, 180),
            new MarketData("MSFT", 101, 110, 90)
    );


    @Before
    public void clearBefore() {
        IOTools.deleteDirWithFiles(Q_NAME);
    }

    @After
    public void clearAfter() {
        IOTools.deleteDirWithFiles(Q_NAME);
    }

    @Test
    public void lastMarketDataPerSymbol() {

        final Accumulation<Map<String, MarketData>> listener = Accumulations.of(
                builder(MarketData.class).build(),
                collectingAndThen(toConcurrentMap(MarketData::symbol, Function.identity(), replacingMerger()), Collections::unmodifiableMap));

        writeToQueue(listener);

        final Map<String, MarketData> expected = MARKET_DATA_SET.stream()
                .collect(toMap(MarketData::symbol, Function.identity(), (a, b) -> b));

        assertEquals(expected, listener.accumulation());
    }

    @Test
    public void symbolSet() {

        Accumulation<Set<String>> listener = Accumulations.of(
                builder(MarketData.class).build().map(MarketData::symbol),
                toConcurrentSet());

        writeToQueue(listener);

        final Set<String> expected = MARKET_DATA_SET.stream()
                .map(MarketData::symbol)
                .collect(toSet());

        assertEquals(expected, listener.accumulation());
    }


    private void writeToQueue(AppenderListener listener) {
        final SetTimeProvider tp = new SetTimeProvider(TimeUnit.DAYS.toNanos(365));
        try (ChronicleQueue q = SingleChronicleQueueBuilder.builder()
                .path(Q_NAME)
                .timeProvider(tp)
                .appenderListener(listener)
                .build()) {
            ExcerptAppender appender = q.acquireAppender();

            MARKET_DATA_SET.forEach(md -> write(appender, md));
        }
    }

    private static void write(ExcerptAppender appender, MarketData marketData) {
        try (final DocumentContext dc = appender.writingDocument()) {
            dc.wire().getValueOut().object(marketData);
        }
    }

}