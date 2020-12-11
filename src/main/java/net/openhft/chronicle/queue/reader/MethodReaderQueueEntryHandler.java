package net.openhft.chronicle.queue.reader;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Mocker;
import net.openhft.chronicle.wire.WireIn;

import java.util.function.Consumer;

@Deprecated /* For removal in x.22, Use QueueEntryHandler.methodReader() instead */
public final class MethodReaderQueueEntryHandler implements QueueEntryHandler {
    private final Class<?> mrInterface;

    public MethodReaderQueueEntryHandler(String methodReaderInterface) {
        try {
            mrInterface = Class.forName(methodReaderInterface);
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