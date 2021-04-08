package net.openhft.chronicle.queue.reader;

import net.openhft.chronicle.queue.internal.reader.InternalDummyMethodReaderQueueEntryHandler;
import net.openhft.chronicle.queue.internal.reader.InternalMessageToTextQueueEntryHandler;
import net.openhft.chronicle.queue.internal.reader.InternalMethodReaderQueueEntryHandler;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface QueueEntryHandler extends BiConsumer<WireIn, Consumer<String>>, AutoCloseable {

    @Override
    void close();

    @NotNull
    static QueueEntryHandler dummy(@NotNull final WireType wireType) {
        return new InternalDummyMethodReaderQueueEntryHandler(wireType);
    }

    @NotNull
    static QueueEntryHandler messageToText(@NotNull final WireType wireType) {
        return new InternalMessageToTextQueueEntryHandler(wireType);
    }

    @NotNull
    static QueueEntryHandler methodReader(@NotNull final String methodReaderInterface) {
        return new InternalMethodReaderQueueEntryHandler(methodReaderInterface);
    }
}