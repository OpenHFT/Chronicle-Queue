package net.openhft.chronicle.queue.incubator.streaming.demo.accumulation;

import net.openhft.chronicle.queue.incubator.streaming.Accumulation;
import net.openhft.chronicle.queue.incubator.streaming.Accumulations;
import net.openhft.chronicle.queue.incubator.streaming.ExcerptExtractor;

import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static net.openhft.chronicle.queue.incubator.streaming.Accumulation.Builder.Accumulator.merging;
import static net.openhft.chronicle.queue.incubator.streaming.Accumulation.Builder.Accumulator.replacingMerger;
import static net.openhft.chronicle.queue.incubator.streaming.ExcerptExtractor.*;
import static net.openhft.chronicle.queue.incubator.streaming.ToLongExcerptExtractor.extractingIndex;

public class DemoTest {

    public static void main(String[] args) {

        // Todo. Move Accumulation to be an outer class.

        // Maintains a count of the number of excerpts encountered
        Accumulation<LongSupplier> count = Accumulations.counting();

        // Maintains a view of the highest index encountered or -1 if now index was encountered
        Accumulation<LongSupplier> maxIndex = Accumulations.reducingLong(extractingIndex(), -1L, Math::max);

        // Maintains a List of all MarketData elements encountered in a List
        Accumulation<List<MarketData>> list = Accumulations.toList(builder(MarketData.class).build());

        // Maintains a Set of all MarketData symbols starting with "S"
        Accumulation<Set<String>> symbolsStartingWithS = Accumulations.toSet(builder(MarketData.class).build()
                .map(MarketData::symbol)
                .filter(s -> s.startsWith("S")));

        // Maintains a Map of the latest MarketData message per symbol where the
        // messages were previously written by a MethodWriter of type MarketDataProvider
        Accumulation<Map<String, MarketData>> latest = Accumulations.toMap(
                merging(
                        builder(MarketData.class).withMethod(MarketDataProvider.class, MarketDataProvider::marketData).build(),
                        MarketData::symbol,
                        Function.identity(),
                        replacingMerger()
                ));


        // Todo: This is not thread-safe:

        // Maintains statistics per symbol on MarketData::last using vanilla Java
        // classes (creates objects). A similar object creation free scheme could reside in QE
        Accumulation<Map<String, DoubleSummaryStatistics>> stats = Accumulations.toMap(
                merging(
                        ExcerptExtractor.builder(MarketData.class).withMethod(MarketDataProvider.class, MarketDataProvider::marketData).build(),
                        MarketData::symbol,
                        md -> {
                            DoubleSummaryStatistics value = new DoubleSummaryStatistics();
                            value.accept(md.last());
                            return value;
                        },
                        (a, b) -> {
                            a.combine(b);
                            return a;
                        }
                ));

        // Todo: fix the impedance mismatch above. See https://docs.oracle.com/javase/10/docs/api/java/util/IntSummaryStatistics.html
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
