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

        // Todo. Move Accumulation to be an outer class.

        // Maintains a count of the number of excerpts encountered
        Reduction<LongSupplier> count = Reductions.counting();

        // Maintains a view of the highest index encountered or 0 if now index was encountered
        Reduction<LongSupplier> maxIndex = Reductions.reducingLong(extractingIndex(), 0L, Math::max);

        // Maintains a List of all MarketData elements encountered in a List
        Reduction<List<MarketData>> list = Reductions.of(builder(MarketData.class).build(), toConcurrentList());

        // Maintains a Set of all MarketData symbols starting with "S"
        Reduction<Set<String>> symbolsStartingWithS = Reductions.of(
                builder(MarketData.class).build()
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


        // Maintains statistics per symbol on MarketData::last using vanilla Java
        // classes (creates objects).
        Reduction<ConcurrentMap<String, DoubleSummaryStatistics>> stats = Reductions.of(
                ExcerptExtractor.builder(MarketData.class)
                        .withMethod(MarketDataProvider.class, MarketDataProvider::marketData)
                        .build(),
                groupingByConcurrent(
                        MarketData::symbol,
                        mapping(MarketData::last,
                                reducing(
                                        new DoubleSummaryStatistics(),
                                        last -> {
                                            DoubleSummaryStatistics value = new DoubleSummaryStatistics();
                                            value.accept(last);
                                            return value;
                                        },
                                        (a, b) -> {
                                            a.combine(b);
                                            return a;
                                        }))));


        // Todo: fix the impedance mismatch above. See https://docs.oracle.com/javase/10/docs/api/java/util/IntSummaryStatistics.html
        // This can be made by mapping to a DoubleExcerptExtractor


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
