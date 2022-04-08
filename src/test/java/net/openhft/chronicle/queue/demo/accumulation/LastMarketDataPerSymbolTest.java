package net.openhft.chronicle.queue.demo.accumulation;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.AppenderListener.Accumulation;
import net.openhft.chronicle.queue.AppenderListener.Accumulation.Builder.Accumulator;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static net.openhft.chronicle.queue.AppenderListener.Accumulation.Builder.Extractor;
import static net.openhft.chronicle.queue.AppenderListener.Accumulation.builder;
import static org.junit.Assert.assertEquals;

public class LastMarketDataPerSymbolTest extends ChronicleQueueTestBase {

    private static final String Q_NAME = LastMarketDataPerSymbolTest.class.getSimpleName();

    private static final List<MarketData> MARKET_DATA_SET = Arrays.asList(
            new MarketData("MSFT", 10, 11, 9),
            new MarketData("MSFT", 100, 110, 90),
            new MarketData("APPL", 200, 220, 180)
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
    public void lastMarketDataPerSymbolCustom() {

        Accumulation<Map<String, MarketData>> listener = builder(ConcurrentHashMap::new, String.class, MarketData.class)
                .withAccumulator(Accumulator.mapping(Extractor.ofType(MarketData.class), MarketData::symbol, Function.identity(), Accumulator.replacingMerger()))
                //.withAccumulator(Accumulator.mappingType(MarketData.class, MarketData::symbol, Function.identity(), Accumulator.replacingMerger()))
                .addViewer(Collections::unmodifiableMap)
                .build();

        writeToQueue(listener);

        final Map<String, MarketData> expected = MARKET_DATA_SET.stream()
                .collect(Collectors.toMap(MarketData::symbol, Function.identity(), (a, b) -> b));

        assertEquals(expected, listener.accumulation());
    }

    @Test
    public void lastMarketDataPerSymbol() {

        Accumulation<Map<String, MarketData>> listener = Accumulations.toMap(
                Accumulator.mapping(Extractor.ofType(MarketData.class), MarketData::symbol, Function.identity(), Accumulator.replacingMerger()));

        writeToQueue(listener);

        final Map<String, MarketData> expected = MARKET_DATA_SET.stream()
                .collect(Collectors.toMap(MarketData::symbol, Function.identity(), (a, b) -> b));

        assertEquals(expected, listener.accumulation());
    }

    @Test
    public void symbolSet() {

        Accumulation<Set<String>> listener = Accumulations.toSet(Extractor.ofType(MarketData.class).map(MarketData::symbol));

        writeToQueue(listener);

        final Set<String> expected = MARKET_DATA_SET.stream()
                .map(MarketData::symbol)
                .collect(Collectors.toSet());

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