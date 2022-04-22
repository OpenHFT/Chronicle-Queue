package net.openhft.chronicle.queue.incubator.streaming.demo.streams;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.incubator.streaming.DocumentExtractor;
import net.openhft.chronicle.queue.incubator.streaming.Streams;
import net.openhft.chronicle.queue.incubator.streaming.ToLongDocumentExtractor;
import net.openhft.chronicle.queue.incubator.streaming.demo.reduction.MarketData;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static net.openhft.chronicle.queue.incubator.streaming.CreateUtil.*;
import static net.openhft.chronicle.queue.incubator.streaming.DocumentExtractor.builder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamsDemoTest {

    static final String Q_NAME = StreamsDemoTest.class.getSimpleName();

    @Test
    void streamTypeMarketDataSimple() {
        ClassAliasPool.CLASS_ALIASES.addAlias(MarketData.class);
        try (SingleChronicleQueue queue = createThenValueOuts(Q_NAME,
                vo -> vo.object(new MarketData("MSFT", 100, 110, 90)),
                vo -> vo.object(new MarketData("AAPL", 200, 220, 180)),
                vo -> vo.object(new MarketData("MSFT", 101, 110, 90))
        )) {

            String s = Streams.of(
                            queue.createTailer(),
                            DocumentExtractor.builder(MarketData.class).build()
                    )
                    .skip(0)
                    // skip 100
                    .limit(50)
                    .map(Object::toString)
                    .collect(Collectors.joining(","));

            assertEquals("!net.openhft.chronicle.queue.incubator.streaming.demo.accumulation.MarketData {\n" +
                    "  symbol: MSFT,\n" +
                    "  last: 100.0,\n" +
                    "  high: 110.0,\n" +
                    "  low: 90.0\n" +
                    "}\n" +
                    ",!net.openhft.chronicle.queue.incubator.streaming.demo.accumulation.MarketData {\n" +
                    "  symbol: AAPL,\n" +
                    "  last: 200.0,\n" +
                    "  high: 220.0,\n" +
                    "  low: 180.0\n" +
                    "}\n" +
                    ",!net.openhft.chronicle.queue.incubator.streaming.demo.accumulation.MarketData {\n" +
                    "  symbol: MSFT,\n" +
                    "  last: 101.0,\n" +
                    "  high: 110.0,\n" +
                    "  low: 90.0\n" +
                    "}\n", s);
        }
    }


    @Test
    void latestIndex() {
        try (SingleChronicleQueue q = createThenValueOuts(Q_NAME,
                vo -> vo.writeLong(1),
                vo -> vo.writeLong(2),
                vo -> vo.writeLong(3)
        )) {
            long last = Streams.ofLong(q.createTailer(), ToLongDocumentExtractor.extractingIndex())
                    .max()
                    .orElse(-1);

            assertEquals("16d00000002", Long.toHexString(last));

        }
    }


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
    void streamTypeMarketData() {
        try (SingleChronicleQueue queue = createThenValueOuts(Q_NAME,
                vo -> vo.object(new MarketData("MSFT", 100, 110, 90)),
                vo -> vo.object(new MarketData("APPL", 200, 220, 180)),
                vo -> vo.object(new MarketData("MSFT", 101, 110, 90))
        )) {


            Map<String, List<MarketData>> groups = Streams.of(queue.createTailer(), builder(MarketData.class).build())
                    .collect(groupingBy(MarketData::symbol));

            Map<String, List<MarketData>> expected = Stream.of(
                            new MarketData("MSFT", 100, 110, 90),
                            new MarketData("APPL", 200, 220, 180),
                            new MarketData("MSFT", 101, 110, 90)
                    )
                    .collect(groupingBy(MarketData::symbol));

            assertEquals(expected, groups);

            DoubleAdder adder = new DoubleAdder();
            Iterator<MarketData> iterator = Streams.iterator(queue.createTailer(), builder(MarketData.class).build());
            iterator.forEachRemaining(md -> adder.add(md.last()));

            assertEquals(401.0, adder.doubleValue(), 1e-10);

        }
    }

    @Test
    void streamType2() {
        try (SingleChronicleQueue q = createThenValueOuts(Q_NAME,
                vo -> vo.object(new Shares("ABCD", 100_000_000)),
                vo -> vo.object(new Shares("EFGH", 200_000_000)),
                vo -> vo.object(new Shares("ABCD", 300_000_000))
        )) {
            Map<String, List<Shares>> groups = Streams.of(q.createTailer(), builder(Shares.class).build())
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

            List<News> newsList = Streams.of(
                            q.createTailer(),
                            DocumentExtractor.builder(News.class).withMethod(Messages.class, Messages::news).build()
                    )
                    .sorted(Comparator.comparing(News::symbol))
                    .collect(toList());

            List<News> expected = Stream.of(firstNews, secondNews)
                    .sorted(Comparator.comparing(News::symbol))
                    .collect(toList());
            assertEquals(expected, newsList);

            final LongSummaryStatistics stat = Streams.of(
                            q.createTailer(),
                            DocumentExtractor.builder(Shares.class).withMethod(Messages.class, Messages::shares).build()
                    )
                    .mapToLong(Shares::noShares)
                    .summaryStatistics();

            LongSummaryStatistics expectedStat = LongStream.of(100_000_000, 200_000_000).summaryStatistics();
            assertLongSummaryStatisticsEqual(expectedStat, stat);

            final LongSummaryStatistics stat2 = Streams.ofLong(q.createTailer(),
                            DocumentExtractor.builder(Shares.class).
                                    withMethod(Messages.class, Messages::shares)
                                    .withReusing(Shares::new)
                                    .build()
                                    .mapToLong(Shares::noShares)
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

            long sum = Streams.ofLong(q.createTailer().disableThreadSafetyCheck(true),
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

            long sum = Streams.of(q.createTailer().disableThreadSafetyCheck(true),
                            builder(Shares.class).build())
                    .parallel()
                    .peek(v -> threads.add(Thread.currentThread()))
                    .mapToLong(Shares::noShares)
                    .sum();

            long expected = (long) (no - 1) * no / 2L;
            assertEquals(expected, sum);

            if (Runtime.getRuntime().availableProcessors() > 2) {
                assertTrue(threads.size() > 1);
            }

        }
    }

    @Test
    @Disabled("Performance test")
    void performance() {
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

            long st = 0;
            long it = 0;
            for (int i = 0; i < 105; i++) {

                ExcerptTailer tailer = q.createTailer().disableThreadSafetyCheck(true);

                final long iterationBegin = System.currentTimeMillis();
                final Shares shares = new Shares();
                long sum = 0;
                for (; ; ) {
                    try (DocumentContext dc = tailer.readingDocument()) {
                        if (dc.isPresent()) {
                            Wire wire = dc.wire();
                            if (wire != null) {
                                Shares s = wire.getValueIn().object(shares, Shares.class);
                                sum += s.noShares();
                                continue;
                            }
                        }
                        break;
                    }
                }

                final long iterationDurationMs = System.currentTimeMillis() - iterationBegin;

                final long streamBegin = System.currentTimeMillis();

                long streamSum = Streams.ofLong(q.createTailer().disableThreadSafetyCheck(true),
                                builder(Shares.class)
                                        .withReusing(Shares::new)
                                        .build()
                                        .mapToLong(Shares::noShares))
                        .sum();

                final long streamDurationMs = System.currentTimeMillis() - streamBegin;

                assertEquals(streamSum, sum);
                System.out.println("streamDurationMs = " + streamDurationMs);
                System.out.println("iterationDurationMs = " + iterationDurationMs);

                if (i > 5) {
                    st += streamDurationMs;
                    it += iterationDurationMs;
                }

            }
            System.out.println("st = " + st);
            System.out.println("it = " + it);

        }
    }

    @Test
    void streamCloseTailer() {
        try (SingleChronicleQueue queue = createThenValueOuts(Q_NAME,
                vo -> vo.object(new MarketData("MSFT", 100, 110, 90)),
                vo -> vo.object(new MarketData("APPL", 200, 220, 180)),
                vo -> vo.object(new MarketData("MSFT", 101, 110, 90))
        )) {

            Map<String, List<MarketData>> groups;
            try (ExcerptTailer tailer = queue.createTailer()) {
                groups = Streams.of(tailer, builder(MarketData.class).build())
                        .collect(groupingBy(MarketData::symbol));
            }
            // A bit sloppy...
            assertEquals(2, groups.size());
        }
    }

    @Test
    void streamObjectReuse() {
        try (SingleChronicleQueue queue = createThenValueOuts(Q_NAME,
                vo -> vo.object(new MarketData("MSFT", 100, 110, 90)),
                vo -> vo.object(new MarketData("AAPL", 200, 220, 180)),
                vo -> vo.object(new MarketData("MSFT", 101, 110, 90))
        )) {

            OptionalDouble max = Streams.of(queue.createTailer(),
                            builder(MarketData.class)
                                    .withReusing(MarketData::new)
                                    .build())
                    .mapToDouble(MarketData::last)
                    .max();

            OptionalDouble expected = OptionalDouble.of(200);

            assertEquals(expected, max);

        }
    }

    @Test
    void streamIllegalObjectReuse() {
        try (SingleChronicleQueue queue = createThenValueOuts(Q_NAME,
                vo -> vo.object(new MarketData("MSFT", 100, 110, 90)),
                vo -> vo.object(new MarketData("AAPL", 200, 220, 180)),
                vo -> vo.object(new MarketData("MSFT", 101, 110, 90))
        )) {

            List<MarketData> list = Streams.of(queue.createTailer(),
                            builder(MarketData.class)
                                    .withReusing(MarketData::new)
                                    .build())
                    .collect(toList());

            // We will see the last entry in all positions
            List<MarketData> expected = Stream.of(
                            new MarketData("MSFT", 101, 110, 90),
                            new MarketData("MSFT", 101, 110, 90),
                            new MarketData("MSFT", 101, 110, 90)
                    )
                    .collect(toList());

            assertEquals(expected, list);

        }
    }


    public interface Messages {

        void shares(Shares shares);

        void news(News news);

        void greeting(String greeting);

    }

    public static final class Shares extends SelfDescribingMarshallable {

        private String symbol;
        private long noShares;

        public Shares() {
        }

        public Shares(@NotNull String symbol, long noShares) {
            this.symbol = symbol;
            this.noShares = noShares;
        }

        public String symbol() {
            return symbol;
        }

        public void symbol(String symbol) {
            this.symbol = symbol;
        }

        public long noShares() {
            return noShares;
        }

        public void noShares(long shares) {
            this.noShares = shares;
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