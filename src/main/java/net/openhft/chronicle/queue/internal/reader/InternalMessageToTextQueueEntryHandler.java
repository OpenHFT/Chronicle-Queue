/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.reader;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.reader.QueueEntryHandler;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireType;

import java.util.function.Consumer;

import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;

/**
 * {@code InternalMessageToTextQueueEntryHandler} is responsible for converting queue entries into text format.
 * <p>
 * It handles both binary and text formats by inspecting the data format indicator, converting binary data
 * using the provided {@link WireType}, and passing the resulting text to the {@code messageHandler}.
 * <p>
 * This handler can be used to transform queue entries into a human-readable format such as JSON or YAML.
 */
public final class InternalMessageToTextQueueEntryHandler implements QueueEntryHandler {
    private final Bytes<?> textConversionTarget = Bytes.allocateElasticOnHeap();
    private final WireType wireType;

    /**
     * Constructs an {@code InternalMessageToTextQueueEntryHandler} with the specified {@link WireType}.
     *
     * @param wireType The wire type used for converting binary data, must not be null
     */
    public InternalMessageToTextQueueEntryHandler(WireType wireType) {
        this.wireType = requireNonNull(wireType);
    }

    /**
     * Determines if the given data is in binary format based on the data format indicator byte.
     *
     * @param dataFormatIndicator The byte representing the data format
     * @return {@code true} if the data is in binary format, {@code false} otherwise
     */
    private static boolean isBinaryFormat(final byte dataFormatIndicator) {
        return dataFormatIndicator < 0;
    }

    /**
     * Processes entries from the provided {@link WireIn}, converts them to text if in binary format,
     * and passes the result to the provided {@code messageHandler}.
     * <p>
     * If the entry is binary, it will be converted using the specified {@link WireType}. Otherwise,
     * the raw text will be passed through directly.
     *
     * @param wireIn         The wire input to process
     * @param messageHandler The handler that processes the converted or raw message text
     */
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

    /**
     * Releases resources used by this handler, particularly the {@code textConversionTarget} buffer.
     */
    @Override
    public void close() {
        textConversionTarget.releaseLast();
    }
}
