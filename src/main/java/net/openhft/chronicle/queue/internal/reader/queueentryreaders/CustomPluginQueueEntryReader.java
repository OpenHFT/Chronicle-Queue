package net.openhft.chronicle.queue.internal.reader.queueentryreaders;

import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.reader.ChronicleReaderPlugin;
import net.openhft.chronicle.queue.reader.MessageConsumer;
import net.openhft.chronicle.wire.DocumentContext;

import java.util.function.Function;

public final class CustomPluginQueueEntryReader extends AbstractTailerPollingQueueEntryReader {

    private final ChronicleReaderPlugin plugin;
    private final MessageConsumer consumer;

    public CustomPluginQueueEntryReader(ExcerptTailer tailer, Function<ExcerptTailer, DocumentContext> pollMethod, ChronicleReaderPlugin plugin,
                                        MessageConsumer consumer) {
        super(tailer, pollMethod);
        this.plugin = plugin;
        this.consumer = consumer;
    }

    @Override
    protected void doRead(DocumentContext documentContext) {
        plugin.onReadDocument(documentContext, value -> consumer.consume(documentContext.index(), value));
    }
}
