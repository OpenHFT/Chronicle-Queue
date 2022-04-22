package net.openhft.chronicle.queue.incubator.streaming.demo.reduction;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.incubator.streaming.DocumentExtractor;
import net.openhft.chronicle.queue.incubator.streaming.Reduction;
import net.openhft.chronicle.queue.incubator.streaming.Reductions;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.Wire;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.function.Function;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toConcurrentMap;
import static net.openhft.chronicle.queue.incubator.streaming.ConcurrentCollectors.replacingMerger;
import static org.junit.Assert.assertEquals;

public class WireTest extends ChronicleQueueTestBase {

    private static final String Q_NAME = WireTest.class.getSimpleName();

    private static final List<MarketData> MARKET_DATA_SET = Arrays.asList(
            new MarketData("MSFT", 10, 11, 9),
            new MarketData("MSFT", 100, 110, 90),
            new MarketData("AAPL", 200, 220, 180)
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
    public void map() {

        Reduction<Map<String, MarketData>> listener = Reductions.of(
                DocumentExtractor.builder(MarketData.class).withMethod(ServiceOut.class, ServiceOut::marketData).build(),
                collectingAndThen(toConcurrentMap(MarketData::symbol, Function.identity(), replacingMerger()), Collections::unmodifiableMap)
        );

        Wire wire = wire();
        listener.accept(wire);

        MarketData expectedSymbol = createMarketData();
        Map<String, MarketData> expected = new HashMap<>();
        expected.put(expectedSymbol.symbol(), expectedSymbol);

        assertEquals(expected, listener.reduction());
        assertEquals("java.util.Collections$UnmodifiableMap", listener.reduction().getClass().getName());
    }

    private Wire wire() {

        final Wire wire = new BinaryWire(Bytes.elasticByteBuffer());
        ServiceOut serviceOut = wire.methodWriter(ServiceOut.class);

        MarketData marketData = createMarketData();
        marketData.last(0);

        serviceOut.marketData(marketData);
        serviceOut.greeting("Bonjour");
        serviceOut.marketData(createMarketData());
        serviceOut.greeting("Guten Tag");
        return wire;
    }

    static MarketData createMarketData() {
        return new MarketData("MSFT", 100, 110, 90);
    }

    public interface ServiceOut {

        void marketData(MarketData marketData);

        void greeting(String greeting);
    }


}