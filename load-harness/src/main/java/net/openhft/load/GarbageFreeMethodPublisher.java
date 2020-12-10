package net.openhft.load;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.MessageHistory;
import net.openhft.chronicle.wire.ValueOut;
import net.openhft.chronicle.wire.Wire;
import net.openhft.load.messages.EightyByteMessage;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

//import net.openhft.chronicle.queue.impl.single.DebugTimestamps;

public final class GarbageFreeMethodPublisher implements MethodDefinition {
    private final Supplier<ExcerptAppender> outputSupplier;

    public GarbageFreeMethodPublisher(final Supplier<ExcerptAppender> outputSupplier) {
        this.outputSupplier = outputSupplier;
    }

    @Override
    public void onEightyByteMessage(final EightyByteMessage message) {

        final ExcerptAppender appender = outputSupplier.get();
       // DebugTimestamps.operationStart(DebugTimestamps.Operation.GET_WRITING_DOCUMENT);
        @NotNull DocumentContext context = appender.writingDocument();
       // DebugTimestamps.operationEnd(DebugTimestamps.Operation.GET_WRITING_DOCUMENT);
        try {
            Wire wire = context.wire();
            // log write
           // DebugTimestamps.operationStart(DebugTimestamps.Operation.WRITE_EVENT);
            try {
                wire.write(MethodReader.HISTORY).marshallable(MessageHistory.get());
                final ValueOut valueOut = wire.writeEventName("onEightyByteMessage");
                valueOut.object(EightyByteMessage.class, message);
                wire.padToCacheAlign();
            } finally {
               // DebugTimestamps.operationEnd(DebugTimestamps.Operation.WRITE_EVENT);
            }
        } finally {

           // DebugTimestamps.operationStart(DebugTimestamps.Operation.CLOSE_CONTEXT);
            try {
                context.close();
            } finally {
               // DebugTimestamps.operationEnd(DebugTimestamps.Operation.CLOSE_CONTEXT);
            }
        }
    }
}