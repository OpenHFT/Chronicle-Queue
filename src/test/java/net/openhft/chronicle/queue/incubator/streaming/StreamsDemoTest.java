package net.openhft.chronicle.queue.incubator.streaming;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static net.openhft.chronicle.queue.incubator.streaming.CreateUtil.createThenAppending;
import static net.openhft.chronicle.queue.incubator.streaming.CreateUtil.createThenValueOuts;
import static net.openhft.chronicle.queue.incubator.streaming.ExcerptExtractor.ofMethod;
import static net.openhft.chronicle.queue.incubator.streaming.ExcerptExtractor.ofType;

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

            System.out.println("stat = " + stat);

        }
    }

    @Test
    void streamType() {
        try (SingleChronicleQueue q = createThenValueOuts(Q_NAME,
                vo -> vo.object(new Shares("ABCD", 100_000_000)),
                vo -> vo.object(new Shares("EFGH", 200_000_000)),
                vo -> vo.object(new Shares("ABCD", 300_000_000))
        )) {
            Map<String, List<Shares>> shares = Streams.of(q.createTailer(), ofType(Shares.class))
                    .collect(groupingBy(Shares::symbol));

            System.out.println("shares = " + shares);
        }
    }


    @Test
    void streamMessageWriter() {
        try (SingleChronicleQueue q = createThenAppending(Q_NAME, appender -> {
            Messages messages = appender.methodWriter(Messages.class);
            messages.news(new News("MSFT", "Microsoft releases Linux Windows", "In a stunning presentation today, ..."));
            messages.shares(new Shares("AGDG", 100_000_000));
            messages.news(new News("APPL", "Apple releases Iphone 23", "Today, Apple released ..."));
            messages.shares(new Shares("AGDG", 200_000_000));
        })) {

            List<News> newsList = Streams.of(q.createTailer(), ofMethod(Messages.class, News.class, Messages::news))
                    .sorted(Comparator.comparing(News::symbol))
                    .collect(toList());

            System.out.println("newsList = " + newsList);

            final LongSummaryStatistics stat = Streams.of(
                            q.createTailer(),
                            ofMethod(Messages.class, Shares.class, Messages::shares)
                    )
                    .mapToLong(Shares::shares)
                    .summaryStatistics();

            System.out.println("stat = " + stat);

            final LongSummaryStatistics stat2 = Streams.ofLong(q.createTailer(),
                            ofMethod(Messages.class, Shares.class, Messages::shares).mapToLong(Shares::shares)
                    )
                    .summaryStatistics();

            System.out.println("stat2 = " + stat2);

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

}