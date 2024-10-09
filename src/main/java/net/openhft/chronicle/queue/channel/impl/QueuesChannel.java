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
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.UnrecoverableTimeoutException;
import net.openhft.chronicle.wire.channel.*;

/**
 * This class implements {@link ChronicleChannel} and represents a composite channel combining both
 * publishing and subscribing functionality. It manages both a publish and a subscribe queue.
 */
@SuppressWarnings("deprecation")
public class QueuesChannel implements ChronicleChannel {

    // Channel for publishing data to the queue
    private final PublishQueueChannel publishQueueChannel;

    // Channel for subscribing to data from the queue
    private final SubscribeQueueChannel subscribeQueueChannel;

    /**
     * Constructor for {@link QueuesChannel}, initializing both publishing and subscribing channels.
     *
     * @param channelCfg      Configuration for the channel.
     * @param handler         Handler for managing channel operations.
     * @param publishQueue    The {@link ChronicleQueue} to which data is published.
     * @param subscribeQueue  The {@link ChronicleQueue} from which data is subscribed.
     */
    public QueuesChannel(ChronicleChannelCfg<?> channelCfg, AbstractHandler<?> handler, ChronicleQueue publishQueue, ChronicleQueue subscribeQueue) {
        publishQueueChannel = new PublishQueueChannel(channelCfg, handler, publishQueue);
        subscribeQueueChannel = new SubscribeQueueChannel(channelCfg, handler, subscribeQueue);
    }

    /**
     * @return The configuration of the channel.
     */
    @Override
    public ChronicleChannelCfg<?> channelCfg() {
        return publishQueueChannel.channelCfg();
    }

    /**
     * @return The outgoing header used in the channel.
     */
    @Override
    public ChannelHeader headerOut() {
        return publishQueueChannel.headerOut();
    }

    /**
     * @return The incoming header used in the channel.
     */
    @Override
    public ChannelHeader headerIn() {
        return subscribeQueueChannel.headerIn();
    }

    /**
     * Closes both the publish and subscribe channels safely.
     */
    @Override
    public void close() {
        Closeable.closeQuietly(
                publishQueueChannel,
                subscribeQueueChannel);
    }

    /**
     * @return {@code true} if either the publish or subscribe channel is closed, otherwise {@code false}.
     */
    @Override
    public boolean isClosed() {
        return publishQueueChannel.isClosed() || subscribeQueueChannel.isClosed();
    }

    /**
     * Reads a document from the subscription queue.
     *
     * @return A {@link DocumentContext} from the subscription channel.
     */
    @Override
    public DocumentContext readingDocument() {
        return subscribeQueueChannel.readingDocument();
    }

    /**
     * Acquires a writing document from the publishing queue.
     *
     * @param metaData Indicates whether the document is metadata.
     * @return A {@link DocumentContext} to write into.
     * @throws UnrecoverableTimeoutException if acquiring the document fails.
     */
    @Override
    public DocumentContext writingDocument(boolean metaData) throws UnrecoverableTimeoutException {
        return publishQueueChannel.writingDocument(metaData);
    }

    /**
     * Acquires a writing document from the publishing queue without committing until ready.
     *
     * @param metaData Indicates whether the document is metadata.
     * @return A {@link DocumentContext} that can be written to.
     * @throws UnrecoverableTimeoutException if acquiring the document fails.
     */
    @Override
    public DocumentContext acquireWritingDocument(boolean metaData) throws UnrecoverableTimeoutException {
        return publishQueueChannel.acquireWritingDocument(metaData);
    }

    /**
     * Sends a test message over the publish channel with the current time in nanoseconds.
     *
     * @param now The current time in nanoseconds.
     */
    @Override
    public void testMessage(long now) {
        publishQueueChannel.testMessage(now);
    }

    /**
     * @return The last test message received over the subscribe channel.
     */
    @Override
    public long lastTestMessage() {
        return subscribeQueueChannel.lastTestMessage();
    }
}
