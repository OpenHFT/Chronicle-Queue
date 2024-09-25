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

package net.openhft.chronicle.queue.channel.impl;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.NoDocumentContext;
import net.openhft.chronicle.wire.UnrecoverableTimeoutException;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.channel.*;
import net.openhft.chronicle.wire.converter.NanoTime;

/**
 * This class implements {@link ChronicleChannel} and represents a subscription channel
 * to read messages from a {@link ChronicleQueue}. It handles both regular messages and
 * test messages in a subscribing role.
 */
@SuppressWarnings("deprecation")
public class SubscribeQueueChannel implements ChronicleChannel {

    // Channel configuration
    private final ChronicleChannelCfg<?> channelCfg;

    // Handler used for processing responses
    private final AbstractHandler<?> pipeHandler;

    // The outgoing channel header
    private final ChannelHeader headerOut;

    // The Chronicle Queue being subscribed to
    private final ChronicleQueue subscribeQueue;

    // Tailer for reading messages from the queue
    private final ExcerptTailer tailer;

    // The timestamp of the last received test message
    private long lastTestMessage;

    /**
     * Constructs a {@link SubscribeQueueChannel} that subscribes to a given queue and processes messages.
     *
     * @param channelCfg     Configuration settings for the channel.
     * @param pipeHandler    Handler to manage the channel's operations.
     * @param subscribeQueue The {@link ChronicleQueue} that the channel subscribes to.
     */
    public SubscribeQueueChannel(ChronicleChannelCfg<?> channelCfg, AbstractHandler<?> pipeHandler, ChronicleQueue subscribeQueue) {
        this.channelCfg = channelCfg;
        this.pipeHandler = pipeHandler;
        this.headerOut = pipeHandler.responseHeader(null);
        this.subscribeQueue = subscribeQueue;
        tailer = subscribeQueue.createTailer();
    }

    /**
     * @return The configuration settings for the channel.
     */
    @Override
    public ChronicleChannelCfg<?> channelCfg() {
        return channelCfg;
    }

    /**
     * @return The outgoing header used in the channel.
     */
    @Override
    public ChannelHeader headerOut() {
        return headerOut;
    }

    /**
     * @return The incoming header processed by the handler.
     */
    @Override
    public ChannelHeader headerIn() {
        return pipeHandler;
    }

    /**
     * Closes the channel and releases resources, including the tailer and queue.
     */
    @Override
    public void close() {
        Closeable.closeQuietly(
                tailer,
                subscribeQueue);
    }

    /**
     * @return {@code true} if the subscription queue is closed, otherwise {@code false}.
     */
    @Override
    public boolean isClosed() {
        return subscribeQueue.isClosed();
    }

    /**
     * Reads a document from the subscription queue.
     * If the document contains metadata for a test message, it processes the test message.
     *
     * @return A {@link DocumentContext} representing the current read document.
     */
    @Override
    public DocumentContext readingDocument() {
        final DocumentContext dc = tailer.readingDocument(true);

        // Handle test messages
        if (dc.isMetaData()) {
            final Wire wire = dc.wire();
            long pos = wire.bytes().readPosition();
            final String event = wire.readEvent(String.class);
            if ("testMessage".equals(event)) {
                final long testMessage = wire.getValueIn().readLong(NanoTime.INSTANCE);
                lastTestMessage = testMessage;
                try (DocumentContext dc2 = writingDocument(true)) {
                    dc2.wire().write("testMessage").writeLong(NanoTime.INSTANCE, testMessage);
                }
                wire.bytes().readPosition(pos);
                return dc;
            }
            dc.close();
            return readingDocument();
        }

        return dc;
    }

    /**
     * Writing documents is not supported for this channel, as it is a subscription-only channel.
     *
     * @param metaData Indicates whether the document is metadata.
     * @return A {@link NoDocumentContext} indicating no document can be written.
     */
    @Override
    public DocumentContext writingDocument(boolean metaData) throws UnrecoverableTimeoutException {
        return NoDocumentContext.INSTANCE;
    }

    /**
     * Acquiring a writing document is not supported for this channel.
     *
     * @param metaData Indicates whether the document is metadata.
     * @return A {@link NoDocumentContext} indicating no document can be acquired for writing.
     */
    @Override
    public DocumentContext acquireWritingDocument(boolean metaData) throws UnrecoverableTimeoutException {
        return NoDocumentContext.INSTANCE;
    }

    /**
     * Test messages are not supported for this channel.
     *
     * @param now The timestamp of the test message.
     */
    @Override
    public void testMessage(long now) {
        // No implementation needed as this is a subscription-only channel
    }

    /**
     * @return The timestamp of the last received test message.
     */
    @Override
    public long lastTestMessage() {
        return lastTestMessage;
    }
}
