package net.openhft.chronicle.queue.demo.accumulation;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.AppenderListener;
import net.openhft.chronicle.queue.AppenderListener.Accumulation;
import net.openhft.chronicle.queue.AppenderListener.Accumulation.Builder.Accumulator;
import net.openhft.chronicle.queue.AppenderListener.Accumulation.Builder.ExcerptExtractor;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static net.openhft.chronicle.queue.AppenderListener.Accumulation.builder;
import static org.junit.Assert.assertEquals;

public class MethodWriterTest extends ChronicleQueueTestBase {

    private static final String Q_NAME = MethodWriterTest.class.getSimpleName();

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
    public void lastSeen() {
        Accumulation<AtomicReference<MarketData>> listener = Accumulation.builder(AtomicReference<MarketData>::new)
                .withAccumulator(
                        Accumulator.reducing(ExcerptExtractor.ofMethod(ServiceOut.class, MarketData.class, ServiceOut::marketData),
                                AtomicReference::set
                        )
                )
                .build();

        writeToQueue(listener);

        MarketData expected = createMarketData();
        MarketData actual = listener.accumulation().get();
        assertEquals(expected, actual);
    }


    @Test
    public void map() {

        Accumulation<Map<String, MarketData>> listener = builder(ConcurrentHashMap::new, String.class, MarketData.class)
                .withAccumulator(
                        Accumulator.mapping(ExcerptExtractor.ofMethod(ServiceOut.class, MarketData.class, ServiceOut::marketData),
                                MarketData::symbol,
                                Function.identity(),
                                Accumulator.replacingMerger())
                )
                .addViewer(Collections::unmodifiableMap)
                .build();

        writeToQueue(listener);
        MarketData expectedSymbol = createMarketData();
        Map<String, MarketData> expected = new HashMap<>();
        expected.put(expectedSymbol.symbol(), expectedSymbol);

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