package net.openhft.chronicle.queue.internal.reader;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Mocker;
import net.openhft.chronicle.queue.reader.QueueEntryHandler;
import net.openhft.chronicle.wire.WireIn;

import java.util.function.Consumer;

import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;

public final class InternalMethodReaderQueueEntryHandler implements QueueEntryHandler {
    private final Class<?> mrInterface;

    public InternalMethodReaderQueueEntryHandler(String methodReaderInterface) {
        try {
            mrInterface = Class.forName(requireNonNull(methodReaderInterface));
        } catch (ClassNotFoundException e) {
            throw Jvm.rethrow(e);
        }
    }

    @Override
    public void accept(final WireIn wireIn, final Consumer<String> messageHandler) {
        MethodReader methodReader = wireIn.methodReader(Mocker.intercepting(mrInterface, "", s -> {
            long hn = wireIn.headerNumber();
            messageHandler.accept("header: " + hn + "\n" + s);
        }));
        while (methodReader.readOne()) {
            // read all
        }
    }

    @Override
    public void close() {
    }
}