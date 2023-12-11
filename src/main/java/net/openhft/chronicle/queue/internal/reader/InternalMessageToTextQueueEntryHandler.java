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

package net.openhft.chronicle.queue.internal.reader;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.reader.QueueEntryHandler;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireType;

import java.util.function.Consumer;

import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;

public final class InternalMessageToTextQueueEntryHandler implements QueueEntryHandler {
    private final Bytes<?> textConversionTarget = Bytes.allocateElasticOnHeap();
    private final WireType wireType;

    public InternalMessageToTextQueueEntryHandler(WireType wireType) {
        this.wireType = requireNonNull(wireType);
    }

    private static boolean isBinaryFormat(final byte dataFormatIndicator) {
        return dataFormatIndicator < 0;
    }

    @Override
    public void accept(final WireIn wireIn, final Consumer<String> messageHandler) {
        final Bytes<?> serialisedMessage = wireIn.bytes();
        final byte dataFormatIndicator = serialisedMessage.readByte(serialisedMessage.readPosition());
        String text;

        if (isBinaryFormat(dataFormatIndicator)) {
            textConversionTarget.clear();
            final BinaryWire binaryWire = new BinaryWire(serialisedMessage);
            binaryWire.copyTo(wireType.apply(textConversionTarget));
            text = textConversionTarget.toString();
        } else {
            text = serialisedMessage.toString();
        }

        messageHandler.accept(text);
    }

    @Override
    public void close() {
        textConversionTarget.releaseLast();
    }
}
