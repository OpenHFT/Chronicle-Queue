package net.openhft.chronicle.queue.benchmark;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.jlbh.JLBH;
import net.openhft.chronicle.core.jlbh.JLBHOptions;
import net.openhft.chronicle.core.jlbh.JLBHTask;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.jetbrains.annotations.NotNull;

import static net.openhft.chronicle.core.Jvm.isResourceTracing;
import static net.openhft.chronicle.core.io.IOTools.deleteDirWithFiles;
import static net.openhft.chronicle.wire.Marshallable.fromString;

public class BookUpdateWithQueueJLBH implements JLBHTask, BookUpdateListener {

    static {
        DtoAlias.init();
        isResourceTracing();
        System.setProperty("dumpCode", "true");
        System.setProperty("jvm.resource.tracing","false");
    }

    private JLBH lth;

    private BookUpdate bookUpdate;
    private BookUpdateListener out;
    private @NotNull MethodReader methodReader;

    public static void main(String[] args) {
        deleteDirWithFiles("tmp");
        @NotNull JLBHOptions jlbhOptions = new JLBHOptions()
                .warmUpIterations(500_000)
                .iterations(500_000)
                .throughput(500_000)
                .accountForCoordinatedOmission(false)
                .runs(10)
                .jlbhTask(new BookUpdateWithQueueJLBH());
        new JLBH(jlbhOptions).start();
        IOTools.deleteDirWithFiles("tmp");
    }

    long timeTaken = 0;

    @Override
    public void run(long startTimeNS) {
        bookUpdate.eventTime(System.nanoTime());
        out.bookUpdate(bookUpdate);
        bookUpdate.eventTime(0);
        methodReader.readOne();
        lth.sample(timeTaken);
    }

    @Override
    public void bookUpdate(BookUpdate bookUpdate) {
        timeTaken = System.nanoTime() - bookUpdate.eventTime();
    }


    @Override
    public void init(JLBH lth) {
        this.lth = lth;
        IOTools.deleteDirWithFiles("tmp");
        ChronicleQueue q = SingleChronicleQueueBuilder.single("tmp").build();
        out = q.acquireAppender().methodWriter(BookUpdateListener.class);
        final ExcerptTailer tailer = q.createTailer();

        methodReader = new BookUpdateWithQueueJLBHMethodReader2(tailer, (s, in) -> {
        }, () -> null, null, this);


        DtoAlias.init();
        final String EXPECTED = "!BookUpdate {\n" +
                "  eventTime: \"2021-05-19T17:00:00\",\n" +
                "  symbol: BTC/USD,\n" +
                "  exchange: BITSTAMP,\n" +
                "  bids: [\n" +
                "    { rate: 43275.95, qty: 1.3 },\n" +
                "    { rate: 43275.79, qty: 0.28860 },\n" +
                "    { rate: 43275.25, qty: 0.00918782 },\n" +
                "    { rate: 43272.79, qty: 0.57740 },\n" +
                "    { rate: 43267.83, qty: 0.45612121 },\n" +
                "    { rate: 43266.97, qty: 0.08 },\n" +
                "    { rate: 43266.92, qty: 0.17326736 },\n" +
                "    { rate: 43266.83, qty: 0.01571651 },\n" +
                "    { rate: 43265.07, qty: 0.42889726 }\n" +
                "  ],\n" +
                "  asks: [\n" +
                "    { rate: 43298.21, qty: 0.28860 },\n" +
                "    { rate: 43301.37, qty: 0.55910 },\n" +
                "    { rate: 43303.66, qty: 0.23102918 },\n" +
                "    { rate: 43303.95, qty: 0.01858281 },\n" +
                "    { rate: 43304.27, qty: 0.23102314 },\n" +
                "    { rate: 43304.79, qty: 0.47202754 },\n" +
                "    { rate: 43304.81, qty: 0.46741657 },\n" +
                "    { rate: 43308.32, qty: 0.34659164 },\n" +
                "    { rate: 43312.5, qty: 0.005 }\n" +
                "  ]\n" +
                "}\n";

        bookUpdate = fromString(BookUpdate.class, EXPECTED);
    }
}