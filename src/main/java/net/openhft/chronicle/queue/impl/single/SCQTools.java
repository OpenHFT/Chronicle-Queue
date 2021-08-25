package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.MessageHistory;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static net.openhft.chronicle.bytes.MethodReader.MESSAGE_HISTORY_METHOD_ID;
import static net.openhft.chronicle.wire.BinaryWireCode.FIELD_NUMBER;

public enum SCQTools {
    ; // none

    @Nullable
    public static MessageHistory readHistory(@NotNull final DocumentContext dc, final MessageHistory history) {
        final Wire wire = dc.wire();

        if (wire == null)
            return null;

        final Object parent = wire.parent();
        wire.parent(null);
        try {
            final Bytes<?> bytes = wire.bytes();

            final byte code = bytes.readByte(bytes.readPosition());
            history.reset();

            return code == (byte) FIELD_NUMBER ?
                    readHistoryFromBytes(wire, history) :
                    readHistoryFromWire(wire, history);
        } finally {
            wire.parent(parent);
        }
    }

    @Nullable
    private static MessageHistory readHistoryFromBytes(@NotNull final Wire wire, final MessageHistory history) {
        if (MESSAGE_HISTORY_METHOD_ID != wire.readEventNumber())
            return null;
        wire.getValueIn().marshallable(history);
        return history;
    }

    @Nullable
    private static MessageHistory readHistoryFromWire(@NotNull final Wire wire, final MessageHistory history) {
        final StringBuilder sb = StoreTailer.SBP.acquireStringBuilder();
        ValueIn valueIn = wire.read(sb);

        if (!MethodReader.HISTORY.contentEquals(sb))
            return null;
        valueIn.marshallable(history);
        return history;
    }
}
