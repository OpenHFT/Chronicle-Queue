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
        final ValueIn historyValue = readHistoryValue(wire);
        if (historyValue == null) {
            return null;
        }
        historyValue.marshallable(history);
        return history;
    }

    @Nullable
    private static ValueIn readHistoryValue(@NotNull final Wire wire) {
        try (final ScopedResource<StringBuilder> stlSb = StoreTailer.SBP.get()) {
            StringBuilder sb = stlSb.get();
            ValueIn valueIn = wire.read(sb);

            if (!MethodReader.HISTORY.contentEquals(sb))
                return null;
            return valueIn;
        }
    }
}
