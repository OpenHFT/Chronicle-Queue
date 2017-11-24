package net.openhft.load;

import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.MessageHistory;
import net.openhft.chronicle.wire.MethodReader;
import net.openhft.chronicle.wire.ValueOut;
import net.openhft.chronicle.wire.Wire;
import net.openhft.load.messages.EightyByteMessage;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

public final class GarbageFreeMethodPublisher implements MethodDefinition {
    private final Supplier<ExcerptAppender> outputSupplier;

    public GarbageFreeMethodPublisher(final Supplier<ExcerptAppender> outputSupplier) {
        this.outputSupplier = outputSupplier;
    }

    @Override
    public void onEightyByteMessage(final EightyByteMessage message) {
        try (@NotNull DocumentContext context = outputSupplier.get().writingDocument()) {
            Wire wire = context.wire();
            wire.write(MethodReader.HISTORY).marshallable(MessageHistory.get());
            final ValueOut valueOut = wire.writeEventName("onEightyByteMessage");
            valueOut.object(EightyByteMessage.class, message);
            wire.padToCacheAlign();
        }
    }
}