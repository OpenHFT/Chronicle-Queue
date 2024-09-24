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
import net.openhft.chronicle.wire.channel.*;
import net.openhft.chronicle.wire.converter.NanoTime;

import static net.openhft.chronicle.queue.impl.single.ThreadLocalAppender.*;

/**
 * This class implements {@link ChronicleChannel} and represents a publishing channel that writes data to a {@link ChronicleQueue}.
 * It provides methods for writing documents, handling headers, and managing the channel's lifecycle.
 */
@SuppressWarnings("deprecation")
public class PublishQueueChannel implements ChronicleChannel {

    // Configuration for the channel
    private final ChronicleChannelCfg<?> channelCfg;

    // Handler for the publishing operations
    private final AbstractHandler<?> publishHandler;

    // Outgoing header used in the channel
    private final ChannelHeader headerOut;

    // The ChronicleQueue that the channel is writing to
    private final ChronicleQueue publishQueue;

    // Tailer to facilitate queue operations
    private final ExcerptTailer tailer;

    /**
     * Constructor for {@link PublishQueueChannel} which initializes the configuration, handler, queue, and tailer.
     *
     * @param channelCfg    Configuration for the channel
     * @param publishHandler Handler for managing publishing operations
     * @param publishQueue  The ChronicleQueue where the messages will be published
     */
    public PublishQueueChannel(ChronicleChannelCfg<?> channelCfg, AbstractHandler<?> publishHandler, ChronicleQueue publishQueue) {
        this.channelCfg = channelCfg;
        this.publishHandler = publishHandler;
        this.headerOut = publishHandler.responseHeader(null);
        this.publishQueue = publishQueue;
        tailer = publishQueue.createTailer();
    }

    /**
     * @return The configuration of the channel.
     */
    @Override
    public ChronicleChannelCfg<?> channelCfg() {
        return channelCfg;
    }

    /**
     * @return The header used for outgoing messages in the channel.
     */
    @Override
    public ChannelHeader headerOut() {
        return headerOut;
    }

    /**
     * @return The header used for incoming messages in the channel, managed by the publish handler.
     */
    @Override
    public ChannelHeader headerIn() {
        return publishHandler;
    }

    /**
     * Closes the tailer and the publish queue safely.
     */
    @Override
    public void close() {
        Closeable.closeQuietly(
                tailer,
                publishQueue);
    }

    /**
     * @return {@code true} if the publish queue is closed, otherwise {@code false}.
     */
    @Override
    public boolean isClosed() {
        return publishQueue.isClosed();
    }

    /**
     * Returns a no-operation {@link DocumentContext} as this channel doesn't handle reading documents directly.
     *
     * @return An instance of {@link NoDocumentContext}.
     */
    @Override
    public DocumentContext readingDocument() {
        return NoDocumentContext.INSTANCE;
    }

    /**
     * Acquires a writing document from the thread-local appender for this queue.
     *
     * @param metaData If {@code true}, the document is treated as metadata.
     * @return A {@link DocumentContext} to write into.
     * @throws UnrecoverableTimeoutException if acquiring the document fails.
     */
    @Override
    public DocumentContext writingDocument(boolean metaData) throws UnrecoverableTimeoutException {
        return acquireThreadLocalAppender(publishQueue).writingDocument(metaData);
    }

    /**
     * Acquires a writing document without committing until ready, from the thread-local appender for this queue.
     *
     * @param metaData If {@code true}, the document is treated as metadata.
     * @return A {@link DocumentContext} that can be written to.
     * @throws UnrecoverableTimeoutException if acquiring the document fails.
     */
    @Override
    public DocumentContext acquireWritingDocument(boolean metaData) throws UnrecoverableTimeoutException {
        return acquireThreadLocalAppender(publishQueue).acquireWritingDocument(metaData);
    }

    /**
     * Sends a test message over the channel with the current time in nanoseconds.
     *
     * @param now The current time in nanoseconds.
     */
    @Override
    public void testMessage(long now) {
        try (DocumentContext dc = writingDocument(true)) {
            dc.wire().write("testMessage").writeLong(NanoTime.INSTANCE, now);
        }
    }

    /**
     * Unsupported operation as this channel does not handle last test message retrieval.
     *
     * @throws UnsupportedOperationException Always throws this exception when called.
     */
    @Override
    public long lastTestMessage() {
        throw new UnsupportedOperationException();
    }
}
