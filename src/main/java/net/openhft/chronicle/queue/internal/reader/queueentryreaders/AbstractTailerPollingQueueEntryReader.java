package net.openhft.chronicle.queue.internal.reader.queueentryreaders;

import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.reader.QueueEntryReader;
import net.openhft.chronicle.wire.DocumentContext;

import java.util.function.Function;

public abstract class AbstractTailerPollingQueueEntryReader implements QueueEntryReader {

    private final ExcerptTailer tailer;
    private final Function<ExcerptTailer, DocumentContext> pollMethod;

    protected AbstractTailerPollingQueueEntryReader(ExcerptTailer tailer, Function<ExcerptTailer, DocumentContext> pollMethod) {
        this.tailer = tailer;
        this.pollMethod = pollMethod;
    }

    @Override
    public final boolean read() {
        try (DocumentContext dc = pollMethod.apply(tailer)) {
            if (!dc.isPresent()) {
                return false;
            }
            doRead(dc);
            return true;
        }
    }

    protected abstract void doRead(DocumentContext documentContext);
}
