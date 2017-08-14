package net.openhft.chronicle.queue.reader;

import net.openhft.chronicle.wire.WireIn;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface QueueEntryHandler extends BiConsumer<WireIn, Consumer<String>>, AutoCloseable {
    @Override
    void close();
}
