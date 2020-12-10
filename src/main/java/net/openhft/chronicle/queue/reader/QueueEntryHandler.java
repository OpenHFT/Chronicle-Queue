package net.openhft.chronicle.queue.reader;

import net.openhft.chronicle.wire.WireIn;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

@Deprecated /* For removal in 2.22, moved to internal */
public interface QueueEntryHandler extends BiConsumer<WireIn, Consumer<String>>, AutoCloseable {
    @Override
    void close();
}
