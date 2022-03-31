package net.openhft.chronicle.queue.internal.reader.queueentryreaders;

import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.reader.MessageConsumer;
import net.openhft.chronicle.queue.reader.QueueEntryHandler;
import net.openhft.chronicle.queue.reader.QueueEntryReader;
import net.openhft.chronicle.wire.DocumentContext;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

public final class VanillaQueueEntryReader implements QueueEntryReader {

    private final ExcerptTailer tailer;
    private final Function<ExcerptTailer, DocumentContext> pollMethod;
    private final QueueEntryHandler messageConverter;
    private final MessageConsumer messageConsumer;

    public VanillaQueueEntryReader(@NotNull ExcerptTailer tailer, @NotNull Function<ExcerptTailer, DocumentContext> pollMethod,
                                   @NotNull QueueEntryHandler messageConverter, @NotNull MessageConsumer messageConsumer) {
        this.tailer = tailer;
        this.pollMethod = pollMethod;
        this.messageConverter = messageConverter;
        this.messageConsumer = messageConsumer;
    }

    @Override
    public boolean read() {
        try (DocumentContext dc = pollMethod.apply(tailer)) {
            if (!dc.isPresent()) {
                return false;
            }

            messageConverter.accept(dc.wire(), val -> messageConsumer.consume(dc.index(), val));
            return true;
        }
    }
}
