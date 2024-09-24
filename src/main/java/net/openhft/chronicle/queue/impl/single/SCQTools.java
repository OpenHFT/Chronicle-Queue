/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.scoped.ScopedResource;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static net.openhft.chronicle.bytes.MethodReader.MESSAGE_HISTORY_METHOD_ID;
import static net.openhft.chronicle.wire.BinaryWireCode.FIELD_NUMBER;

/**
 * SCQTools class provides utility methods for reading message history
 * from a DocumentContext. It uses different methods to handle cases
 * where the history can be read from wire or bytes.
 */
@SuppressWarnings("deprecation")
public enum SCQTools {
    ; // No instances, utility methods only

    /**
     * Reads the message history from the provided {@link DocumentContext} and updates the given
     * {@link MessageHistory} object. Depending on the data format, it delegates to either reading
     * from bytes or wire.
     *
     * @param dc the DocumentContext from which to read the message history
     * @param history the MessageHistory object to update
     * @return the updated MessageHistory, or null if not available
     */
    @Nullable
    public static MessageHistory readHistory(@NotNull final DocumentContext dc, final MessageHistory history) {
        final Wire wire = dc.wire();

        // If wire is null, no history can be read
        if (wire == null)
            return null;

        final Object parent = wire.parent();
        wire.parent(null);
        try {
            final Bytes<?> bytes = wire.bytes();

            // Check the field number to determine the format
            final byte code = bytes.readByte(bytes.readPosition());
            history.reset();

            return code == (byte) FIELD_NUMBER ?
                    readHistoryFromBytes(wire, history) :
                    readHistoryFromWire(wire, history);
        } finally {
            // Restore the parent object after reading
            wire.parent(parent);
        }
    }

    /**
     * Reads message history from wire bytes if the field number format is used.
     * Verifies that the event number matches the MESSAGE_HISTORY_METHOD_ID before reading.
     *
     * @param wire the Wire object to read from
     * @param history the MessageHistory object to update
     * @return the updated MessageHistory, or null if the method ID doesn't match
     */
    @Nullable
    private static MessageHistory readHistoryFromBytes(@NotNull final Wire wire, final MessageHistory history) {
        // Check if the event number matches the expected MESSAGE_HISTORY_METHOD_ID
        if (MESSAGE_HISTORY_METHOD_ID != wire.readEventNumber())
            return null;

        // Deserialize the message history into the provided object
        wire.getValueIn().marshallable(history);
        return history;
    }

    /**
     * Reads message history from wire if it's not using the field number format.
     * It relies on reading a marshallable value directly from the wire.
     *
     * @param wire the Wire object to read from
     * @param history the MessageHistory object to update
     * @return the updated MessageHistory, or null if history is not present
     */
    @Nullable
    private static MessageHistory readHistoryFromWire(@NotNull final Wire wire, final MessageHistory history) {
        // Read the history value from the wire
        final ValueIn historyValue = readHistoryValue(wire);
        if (historyValue == null)
            return null;

        // Deserialize the message history from the value
        historyValue.marshallable(history);
        return history;
    }

    /**
     * Reads the history value from the wire. The method checks if the content
     * matches the expected history key before proceeding with the read operation.
     *
     * @param wire the Wire object to read from
     * @return the ValueIn object containing the history, or null if the key doesn't match
     */
    @Nullable
    private static ValueIn readHistoryValue(@NotNull final Wire wire) {
        // Use ScopedResource to handle a reusable StringBuilder
        try (final ScopedResource<StringBuilder> stlSb = StoreTailer.SBP.get()) {
            StringBuilder sb = stlSb.get();

            // Read into the StringBuilder and verify the history key
            ValueIn valueIn = wire.read(sb);

            if (!MethodReader.HISTORY.contentEquals(sb))
                return null;

            return valueIn;
        }
    }
}
