package net.openhft.chronicle.queue.incubator.streaming.demo.accumulation;

import net.openhft.chronicle.queue.incubator.streaming.ExcerptExtractor;
import net.openhft.chronicle.queue.incubator.streaming.Reduction;
import net.openhft.chronicle.queue.incubator.streaming.Reductions;

import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static java.util.stream.Collectors.*;
import static net.openhft.chronicle.queue.incubator.streaming.ConcurrentCollectors.*;
import static net.openhft.chronicle.queue.incubator.streaming.ExcerptExtractor.builder;
import static net.openhft.chronicle.queue.incubator.streaming.ToLongExcerptExtractor.extractingIndex;

public class DemoTest {

    public static void main(String[] args) {

        // Maintains a count of the number of excerpts encountered
        Reduction<LongSupplier> counting = Reductions.counting();
        // ...
        long count = counting.reduction().getAsLong();

        // Maintains a view of the highest index encountered or 0 if now index was encountered
        Reduction<LongSupplier> maxIndexing = Reductions.reducingLong(extractingIndex(), 0L, Math::max);

        long maxIndex = maxIndexing.reduction().getAsLong();

        // Maintains a List of all MarketData elements encountered in a List
        Reduction<List<MarketData>> list = Reductions.of(builder(MarketData.class).build(), toConcurrentList());

        // Maintains a Set of all MarketData symbols starting with "S"
        Reduction<Set<String>> symbolsStartingWithS = Reductions.of(
                builder(MarketData.class).withReusing(MarketData::new).build()
                        .map(MarketData::symbol)
                        .filter(s -> s.startsWith("S")),
                toConcurrentSet());

        // Maintains a Map of the latest MarketData message per symbol where the
        // messages were previously written by a MethodWriter of type MarketDataProvider
        Reduction<Map<String, MarketData>> latest = Reductions.of(
                builder(MarketData.class)
                        .withMethod(MarketDataProvider.class, MarketDataProvider::marketData)
                        .build(),
                toConcurrentMap(MarketData::symbol, Function.identity(), replacingMerger()));

        MarketData latestAppleMarketData = latest.reduction().get("AAPL");

        // This creates a live view of the reduction.
        Map<String, MarketData> liveQueueBackedMap = latest.reduction();

        // Maintains statistics per symbol on MarketData::last using vanilla Java
        // classes (creates objects).
        Reduction<ConcurrentMap<String, DoubleSummaryStatistics>> stats = Reductions.of(
                ExcerptExtractor.builder(MarketData.class)
                        .withMethod(MarketDataProvider.class, MarketDataProvider::marketData)
                        .build(),
                groupingByConcurrent(
                        MarketData::symbol,
                        summarizingDouble(MarketData::last)
                )
        );

        double averageApplePrice = stats.reduction().get("AAPL").getAverage();

        // How to use thread confined objects?

    }


    public interface MarketDataProvider {

        void marketData(MarketData marketData);

        void news(String news);

        void action(Action action);

        enum Action {
            OPEN, CLOSE;
        }


    }

}
