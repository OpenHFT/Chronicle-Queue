package net.openhft.chronicle.queue.incubator.streaming.demo.accumulation;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.AppenderListener;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.incubator.streaming.ExcerptExtractor;
import net.openhft.chronicle.queue.incubator.streaming.Reduction;
import net.openhft.chronicle.queue.incubator.streaming.Reductions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collector;

import static java.util.stream.Collectors.*;
import static net.openhft.chronicle.queue.incubator.streaming.ConcurrentCollectors.*;
import static org.junit.Assert.assertEquals;

public class CollectorTest extends ChronicleQueueTestBase {

    private static final String Q_NAME = CollectorTest.class.getSimpleName();

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
    public void lastSeenManual() {

        Collector<MarketData, AtomicReference<MarketData>, MarketData> lastSeen = Collector.of(
                AtomicReference::new,
                AtomicReference::set,
                (a, b) -> a,
                AtomicReference::get,
                Collector.Characteristics.CONCURRENT
        );

        Reduction<MarketData> listener = Reductions.of(
                ExcerptExtractor.builder(MarketData.class).withMethod(ServiceOut.class, ServiceOut::marketData).build(),
                lastSeen
        );

        writeToQueue(listener);

        MarketData expected = createMarketData();
        MarketData actual = listener.reduction();
        assertEquals(expected, actual);
    }

    @Test
    public void lastSeen() {

        Reduction<Optional<MarketData>> listener = Reductions.of(
                ExcerptExtractor.builder(MarketData.class).withMethod(ServiceOut.class, ServiceOut::marketData).build(),
                reducingConcurrent(replacingMerger())
        );

        writeToQueue(listener);

        MarketData expected = createMarketData();
        MarketData actual = listener.reduction().orElseThrow(NoSuchElementException::new);
        assertEquals(expected, actual);
    }

    @Test
    public void map() {

        Reduction<Map<String, MarketData>> listener = Reductions.of(
                ExcerptExtractor.builder(MarketData.class).withMethod(ServiceOut.class, ServiceOut::marketData).build(),
                collectingAndThen(toConcurrentMap(MarketData::symbol, Function.identity(), replacingMerger()), Collections::unmodifiableMap)
        );

        writeToQueue(listener);

        MarketData expectedSymbol = createMarketData();
        Map<String, MarketData> expected = new HashMap<>();
        expected.put(expectedSymbol.symbol(), expectedSymbol);

        assertEquals(expected, listener.reduction());
        assertEquals("java.util.Collections$UnmodifiableMap", listener.reduction().getClass().getName());
    }


    @Test
    public void composite() {

        final Reduction<Map<String, List<Double>>> listener = Reductions.of(
                ExcerptExtractor.builder(MarketData.class).withMethod(ServiceOut.class, ServiceOut::marketData).build(),
                groupingByConcurrent(MarketData::symbol, mapping(MarketData::last, toList()))
        );

        writeToQueue(listener);
        MarketData expectedSymbol = createMarketData();

        Map<String, List<Double>> expected = new HashMap<>();
        expected.put(expectedSymbol.symbol(), Arrays.asList(0D, expectedSymbol.last()));

        assertEquals(expected, listener.reduction());
    }


    private void writeToQueue(AppenderListener listener) {
        final SetTimeProvider tp = new SetTimeProvider(TimeUnit.DAYS.toNanos(365));
        try (ChronicleQueue q = SingleChronicleQueueBuilder.builder()
                .path(Q_NAME)
                .timeProvider(tp)
                .appenderListener(listener)
                .build()) {
            ExcerptAppender appender = q.acquireAppender();

            ServiceOut serviceOut = q.methodWriter(ServiceOut.class);

            MarketData marketData = createMarketData();
            marketData.last(0);

            serviceOut.marketData(marketData);
            serviceOut.greeting("Bonjour");
            serviceOut.marketData(createMarketData());
            serviceOut.greeting("Guten Tag");
        }
    }

    static MarketData createMarketData() {
        return new MarketData("MSFT", 100, 110, 90);
    }

    public interface ServiceOut {

        void marketData(MarketData marketData);

        void greeting(String greeting);
    }


}