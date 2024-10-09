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

package net.openhft.chronicle.queue.channel;

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.bytes.SyncMode;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.SimpleCloseable;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.channel.impl.SubscribeQueueChannel;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.channel.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Consumer;
import java.util.function.Predicate;

import static net.openhft.chronicle.queue.channel.PipeHandler.newQueue;

/**
 * The {@code SubscribeHandler} class is responsible for managing data subscriptions from a Chronicle queue and writing
 * the data into a Chronicle channel. It processes each message, applies a filter if necessary, and forwards the
 * filtered messages to the channel.
 */
@SuppressWarnings("deprecation")
public class SubscribeHandler extends AbstractHandler<SubscribeHandler> {

    // No-op consumer for cases where no index control is needed.
    private static class NoOp extends SelfDescribingMarshallable implements Consumer<ExcerptTailer> {
        @Override
        public void accept(ExcerptTailer o) {
            return;
        }
    }

    // Default no-op consumer instance
    public final static Consumer<ExcerptTailer> NO_OP = new NoOp();

    // Queue name for subscription
    private String subscribe;

    // Whether to close the handler after execution ends
    private transient boolean closeWhenRunEnds = true;

    // Synchronization mode for the queue
    private SyncMode syncMode;

    // Predicate filter to apply to the data before forwarding
    private Predicate<Wire> filter;

    // Source ID for the subscription
    private int sourceId;

    // Consumer to control where the subscription starts
    private Consumer<ExcerptTailer> subscriptionIndexController = NO_OP;

    /**
     * The method to handle the subscription queue tailer.
     * It reads messages from the queue and forwards them to the {@link ChronicleChannel}, applying the filter if provided.
     *
     * @param pauser                   Pauser to manage delays
     * @param channel                  The channel to send data to
     * @param subscribeQueue           The queue to read data from
     * @param filter                   A filter to apply to the data before writing to the channel
     * @param subscriptionIndexController A consumer that controls where the subscription tailer starts
     */
    static void queueTailer(@NotNull Pauser pauser,
                            @NotNull ChronicleChannel channel,
                            @NotNull ChronicleQueue subscribeQueue,
                            @Nullable Predicate<Wire> filter,
                            @NotNull Consumer<ExcerptTailer> subscriptionIndexController) {
        try (ChronicleQueue subscribeQ = subscribeQueue; // leave here so it gets closed
             ExcerptTailer tailer = subscribeQ.createTailer()) {

            // Assume thread safety is handled externally
            tailer.singleThreadedCheckDisabled(true);
            subscriptionIndexController.accept(tailer);

            // Loop until the channel is closing
            while (!channel.isClosing()) {
                if (copyOneMessage(channel, tailer, filter))
                    pauser.reset();
                else
                    pauser.pause();
            }
        } catch (Exception e) {
            Thread.yield();
            if (channel.isClosing() || subscribeQueue.isClosing())
                return;
            throw e;
        }
    }

    /**
     * Copies a single message from the tailer to the channel, applying the filter if needed.
     *
     * @param channel The channel to write data to
     * @param tailer  The ExcerptTailer reading from the subscription queue
     * @param filter  An optional filter to decide which messages to forward
     * @return {@code true} if a message was copied, otherwise {@code false}
     */
    static boolean copyOneMessage(ChronicleChannel channel, ExcerptTailer tailer, Predicate<Wire> filter) {
        try (DocumentContext dc = tailer.readingDocument()) {
            if (!dc.isPresent()) {
                return false;
            }
            if (dc.isMetaData()) {
                return true;
            }

            Wire wire1 = dc.wire();

            // Apply filter if present
            if (filter != null) {
                long pos = wire1.bytes().readPosition();
                if (!filter.test(wire1)) {
                    wire1.bytes().readPosition(wire1.bytes().readLimit());
                    return true;
                }
                wire1.bytes().readPosition(pos);
            }

            // Copy the message to the channel
            try (DocumentContext dc2 = channel.writingDocument()) {
                Wire wire2 = dc2.wire();
                wire1.copyTo(wire2);

                // Check if data buffer size is below the threshold
                final long dataBuffered = wire2.bytes().writePosition();
                // wait for it to drain
                return dataBuffered < 32 << 10;
            }
        }
    }

    public String subscribe() {
        return subscribe;
    }

    /**
     * Sets the name of the queue to subscribe to.
     *
     * @param subscribe The name of the subscription queue.
     * @return This {@link SubscribeHandler} instance.
     */
    public SubscribeHandler subscribe(String subscribe) {
        this.subscribe = subscribe;
        return this;
    }

    public SyncMode syncMode() {
        return syncMode;
    }

    /**
     * Sets the synchronization mode for the subscription.
     *
     * @param syncMode The {@link SyncMode} for the subscription queue.
     * @return This {@link SubscribeHandler} instance.
     */
    public SubscribeHandler syncMode(SyncMode syncMode) {
        this.syncMode = syncMode;
        return this;
    }

    public Predicate<Wire> filter() {
        return filter;
    }

    /**
     * Sets a filter to be applied to the messages read from the subscription queue.
     *
     * @param filter A {@link Predicate} to filter messages before sending them to the channel.
     * @return This {@link SubscribeHandler} instance.
     */
    public SubscribeHandler filter(Predicate<Wire> filter) {
        this.filter = filter;
        return this;
    }

    @SuppressWarnings("try")
    @Override
    public void run(ChronicleContext context, ChronicleChannel channel) {
        Pauser pauser = Pauser.balanced();

        final ExcerptTailer tailer;

        try (ChronicleQueue subscribeQ = newQueue(context, subscribe, syncMode, sourceId)) {
            InternalChronicleChannel icc = (InternalChronicleChannel) channel;
            if (icc.supportsEventPoller()) {
                // Event polling is supported, create a tailer for polling
                tailer = subscribeQ.createTailer();
                icc.eventPoller(new SHEventHandler(tailer, filter));
                closeWhenRunEnds = false;
            } else {
                // Regular subscription flow with affinity lock
                try (AffinityLock lock = context.affinityLock()) {
                    queueTailer(pauser, channel, newQueue(context, subscribe, syncMode, sourceId), filter, subscriptionIndexController);
                }
                closeWhenRunEnds = true;
            }
        }
    }

    @Override
    public boolean closeWhenRunEnds() {
        return closeWhenRunEnds;
    }

    /**
     * Creates an internal channel based on the current context and configuration.
     *
     * @param context The {@link ChronicleContext} to use.
     * @param channelCfg The configuration for the {@link ChronicleChannel}.
     * @return A new {@link SubscribeQueueChannel} for the current context.
     */
    @Override
    public ChronicleChannel asInternalChannel(ChronicleContext context, ChronicleChannelCfg<?> channelCfg) {
        return new SubscribeQueueChannel(channelCfg, this, newQueue(context, subscribe, syncMode, sourceId));
    }

    /**
     * Event handler for polling events from the subscription queue.
     */
    static class SHEventHandler extends SimpleCloseable implements EventPoller {
        private final ExcerptTailer tailer;
        private final Predicate<Wire> filter;

        SHEventHandler(ExcerptTailer tailer, Predicate<Wire> filter) {
            this.tailer = tailer;
            this.filter = filter;
        }

        /**
         * Polls for events from the tailer and forwards them to the channel if applicable.
         *
         * @param channel The {@link ChronicleChannel} to send data to.
         * @return {@code true} if a message was forwarded, otherwise {@code false}.
         */
        @Override
        public boolean onPoll(ChronicleChannel channel) {
            boolean wrote = false;
            while (copyOneMessage(channel, tailer, filter))
                wrote = true;
            return wrote;
        }

        @Override
        protected void performClose() {
            super.performClose();
            Closeable.closeQuietly(tailer);
        }
    }

    /**
     * Sets the source ID for the subscription queue.
     *
     * @param sourceId The source ID to set for the subscription.
     * @return This {@link SubscribeHandler} instance.
     */
    public SubscribeHandler subscribeSourceId(int sourceId) {
        this.sourceId = sourceId;
        return this;
    }

    /**
     * Sets a consumer to control where the subscription starts from, allowing customization of the first read location.
     *
     * @param subscriptionIndexController A consumer that uses {@link net.openhft.chronicle.queue.ExcerptTailer#moveToIndex(long)}
     *                                    to control the starting read location of the tailer.
     * @return This {@link SubscribeHandler} instance.
     */
    public SubscribeHandler subscriptionIndexController(Consumer<ExcerptTailer> subscriptionIndexController) {
        this.subscriptionIndexController = subscriptionIndexController;
        return this;
    }
}
