package net.openhft.chronicle.queue.incubator.streaming.demo.streams;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.incubator.streaming.Streams;
import net.openhft.chronicle.queue.incubator.streaming.demo.accumulation.MarketData;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static net.openhft.chronicle.queue.incubator.streaming.CreateUtil.*;
import static net.openhft.chronicle.queue.incubator.streaming.ExcerptExtractor.ofMethod;
import static net.openhft.chronicle.queue.incubator.streaming.ExcerptExtractor.ofType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamsDemoTest {

    static final String Q_NAME = StreamsDemoTest.class.getSimpleName();

    @Test
    void streamRaw() {
        try (SingleChronicleQueue q = createThenValueOuts(Q_NAME,
                vo -> vo.writeLong(1),
                vo -> vo.writeLong(2),
                vo -> vo.writeLong(3)
        )) {
            LongSummaryStatistics stat = Streams.ofLong(q.createTailer(),
                            (wire, index) -> wire.getValueIn().readLong())
                    .summaryStatistics();

            LongSummaryStatistics expected = LongStream.of(1, 2, 3).summaryStatistics();
            assertLongSummaryStatisticsEqual(expected, stat);
        }
    }

    @Test
    void streamTypeMarketDataSimple() {
        try (SingleChronicleQueue queue = createThenValueOuts(Q_NAME,
                vo -> vo.object(new MarketData("MSFT", 100, 110, 90)),
                vo -> vo.object(new MarketData("APPL", 200, 220, 180)),
                vo -> vo.object(new MarketData("MSFT", 101, 110, 90))
        )) {

            String s = Streams.of(queue.createTailer(), ofType(MarketData.class))
                    .skip(100)
                    .limit(50)
                    .map(Object::toString)
                    .collect(Collectors.joining(","));

            System.out.println(s);

        }
    }

    @Test
    void streamTypeMarketData() {
        try (SingleChronicleQueue queue = createThenValueOuts(Q_NAME,
                vo -> vo.object(new MarketData("MSFT", 100, 110, 90)),
                vo -> vo.object(new MarketData("APPL", 200, 220, 180)),
                vo -> vo.object(new MarketData("MSFT", 101, 110, 90))
        )) {


            Map<String, List<MarketData>> groups = Streams.of(queue.createTailer(), ofType(MarketData.class))
                    .collect(groupingBy(MarketData::symbol));

            Map<String, List<MarketData>> expected = Stream.of(
                            new MarketData("MSFT", 100, 110, 90),
                            new MarketData("APPL", 200, 220, 180),
                            new MarketData("MSFT", 101, 110, 90)
                    )
                    .collect(groupingBy(MarketData::symbol));

            assertEquals(expected, groups);
        }
    }

    @Test
    void streamType2() {
        try (SingleChronicleQueue q = createThenValueOuts(Q_NAME,
                vo -> vo.object(new Shares("ABCD", 100_000_000)),
                vo -> vo.object(new Shares("EFGH", 200_000_000)),
                vo -> vo.object(new Shares("ABCD", 300_000_000))
        )) {
            Map<String, List<Shares>> groups = Streams.of(q.createTailer(), ofType(Shares.class))
                    .collect(groupingBy(Shares::symbol));

            Map<String, List<Shares>> expected = new HashMap<>();
            expected.put("ABCD", Arrays.asList(new Shares("ABCD", 100_000_000), new Shares("ABCD", 300_000_000)));
            expected.put("EFGH", Collections.singletonList(new Shares("EFGH", 200_000_000)));
            assertEquals(expected, groups);
        }
    }


    @Test
    void streamMessageWriter() {
        News firstNews = new News("MSFT", "Microsoft releases Linux Windows", "In a stunning presentation today, ...");
        News secondNews = new News("APPL", "Apple releases Iphone 23", "Today, Apple released ...");

        try (SingleChronicleQueue q = createThenAppending(Q_NAME, appender -> {
            Messages messages = appender.methodWriter(Messages.class);
            messages.news(firstNews);
            messages.shares(new Shares("AGDG", 100_000_000));
            messages.news(secondNews);
            messages.shares(new Shares("AGDG", 200_000_000));
        })) {

            List<News> newsList = Streams.of(q.createTailer(), ofMethod(Messages.class, News.class, Messages::news))
                    .sorted(Comparator.comparing(News::symbol))
                    .collect(toList());

            List<News> expected = Stream.of(firstNews, secondNews)
                    .sorted(Comparator.comparing(News::symbol))
                    .collect(toList());
            assertEquals(expected, newsList);

            final LongSummaryStatistics stat = Streams.of(
                            q.createTailer(),
                            ofMethod(Messages.class, Shares.class, Messages::shares)
                    )
                    .mapToLong(Shares::shares)
                    .summaryStatistics();

            LongSummaryStatistics expectedStat = LongStream.of(100_000_000, 200_000_000).summaryStatistics();
            assertLongSummaryStatisticsEqual(expectedStat, stat);

            final LongSummaryStatistics stat2 = Streams.ofLong(q.createTailer(),
                            ofMethod(Messages.class, Shares.class, Messages::shares).mapToLong(Shares::shares)
                    )
                    .summaryStatistics();

            assertLongSummaryStatisticsEqual(expectedStat, stat2);
        }
    }

    @Test
    void longStreamParallel() {
        final int no = 100_000;

        try (SingleChronicleQueue q = create(Q_NAME)) {
            ExcerptAppender appender = q.acquireAppender();
            for (int i = 0; i < no; i++) {
                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire()
                            .getValueOut()
                            .writeLong(i);
                }
            }

            Set<Thread> threads = Collections.newSetFromMap(new ConcurrentHashMap<>());

            long sum = Streams.ofLong(q.createTailer(),
                            (wire, index) -> wire.getValueIn().readLong())
                    .parallel()
                    .peek(v -> threads.add(Thread.currentThread()))
                    .sum();

            long expected = (long) no * no / 2L;
            assertEquals(expected, sum);

            System.out.println("threads = " + threads);

        }
    }

    @Test
    void streamParallel() {
        final int no = 100_000;

        try (SingleChronicleQueue q = create(Q_NAME)) {
            ExcerptAppender appender = q.acquireAppender();
            for (int i = 0; i < no; i++) {
                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire()
                            .getValueOut()
                            .object(new Shares("AGDG", i));
                }
            }

            Set<Thread> threads = Collections.newSetFromMap(new ConcurrentHashMap<>());

            long sum = Streams.of(q.createTailer(),
                            ofType(Shares.class))
                    .parallel()
                    .peek(v -> threads.add(Thread.currentThread()))
                    .mapToLong(Shares::shares)
                    .sum();

            long expected = (long) (no - 1) * no / 2L;
            assertEquals(expected, sum);

            System.out.println("threads = " + threads);

        }
    }


    public interface Messages {

        void shares(Shares shares);

        void news(News news);

        void greeting(String greeting);

    }

    public static final class Shares extends SelfDescribingMarshallable {

        private String symbol;
        private long shares;

        public Shares() {
        }

        public Shares(@NotNull String symbol, long shares) {
            this.symbol = symbol;
            this.shares = shares;
        }

        public String symbol() {
            return symbol;
        }

        public void symbol(String symbol) {
            this.symbol = symbol;
        }

        public long shares() {
            return shares;
        }

        public void shares(long shares) {
            this.shares = shares;
        }
    }

    public static final class News extends SelfDescribingMarshallable {

        private String symbol;
        private String header;
        private String body;

        public News() {
        }

        public News(String symbol, String header, String body) {
            this.symbol = symbol;
            this.header = header;
            this.body = body;
        }

        public String symbol() {
            return symbol;
        }

        public void symbol(String symbol) {
            this.symbol = symbol;
        }

        public String header() {
            return header;
        }

        public void header(String header) {
            this.header = header;
        }

        public String body() {
            return body;
        }

        public void body(String body) {
            this.body = body;
        }
    }


    @BeforeEach
    void beforeEach() {
        cleanup();
    }

    @AfterEach
    void afterEach() {
        cleanup();
    }

    void cleanup() {
        IOTools.deleteDirWithFiles(Q_NAME);
    }

    private static void assertLongSummaryStatisticsEqual(LongSummaryStatistics a,
                                                         LongSummaryStatistics b) {

        assertTrue(Stream.<Function<LongSummaryStatistics, Object>>of(
                LongSummaryStatistics::getCount,
                LongSummaryStatistics::getMin,
                LongSummaryStatistics::getMax,
                LongSummaryStatistics::getSum,
                LongSummaryStatistics::getAverage
        ).allMatch(op -> op.apply(a).equals(op.apply(b))));
    }

}