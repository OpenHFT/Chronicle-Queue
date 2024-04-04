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

public class SubscribeHandler extends AbstractHandler<SubscribeHandler> {

    private static class NoOp extends SelfDescribingMarshallable implements Consumer {
        @Override
        public void accept(Object o) {
            return;
        }
    }

    public final static Consumer NO_OP = new NoOp();

    private String subscribe;
    private transient boolean closeWhenRunEnds = true;

    private SyncMode syncMode;

    private Predicate<Wire> filter;
    private int sourceId;

    private Consumer<ExcerptTailer> subscriptionIndexController = NO_OP;

    static void queueTailer(@NotNull Pauser pauser,
                            @NotNull ChronicleChannel channel,
                            @NotNull ChronicleQueue subscribeQueue,
                            @Nullable Predicate<Wire> filter,
                            @NotNull Consumer<ExcerptTailer> subscriptionIndexController) {
        try (ChronicleQueue subscribeQ = subscribeQueue; // leave here so it gets closed
             ExcerptTailer tailer = subscribeQ.createTailer()) {

            tailer.singleThreadedCheckDisabled(true);  // assume we are thread safe
            subscriptionIndexController.accept(tailer);

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

    static boolean copyOneMessage(ChronicleChannel channel, ExcerptTailer tailer, Predicate<Wire> filter) {
        try (DocumentContext dc = tailer.readingDocument()) {
            if (!dc.isPresent()) {
                return false;
            }
            if (dc.isMetaData()) {
                return true;
            }

            Wire wire1 = dc.wire();
            if (filter != null) {
                long pos = wire1.bytes().readPosition();
                if (!filter.test(wire1)) {
                    wire1.bytes().readPosition(wire1.bytes().readLimit());
                    return true;
                }
                wire1.bytes().readPosition(pos);
            }

            try (DocumentContext dc2 = channel.writingDocument()) {
                Wire wire2 = dc2.wire();
                wire1.copyTo(wire2);

                final long dataBuffered = wire2.bytes().writePosition();
                // wait for it to drain
                return dataBuffered < 32 << 10;
            }
        }
    }

    public String subscribe() {
        return subscribe;
    }

    public SubscribeHandler subscribe(String subscribe) {
        this.subscribe = subscribe;
        return this;
    }

    public SyncMode syncMode() {
        return syncMode;
    }

    public SubscribeHandler syncMode(SyncMode syncMode) {
        this.syncMode = syncMode;
        return this;
    }

    public Predicate<Wire> filter() {
        return filter;
    }

    public SubscribeHandler filter(Predicate<Wire> filter) {
        this.filter = filter;
        return this;
    }

    @Override
    public void run(ChronicleContext context, ChronicleChannel channel) {
        Pauser pauser = Pauser.balanced();

        final ExcerptTailer tailer;

        try (ChronicleQueue subscribeQ = newQueue(context, subscribe, syncMode, sourceId)) {
            InternalChronicleChannel icc = (InternalChronicleChannel) channel;
            if (icc.supportsEventPoller()) {
                tailer = subscribeQ.createTailer();
                icc.eventPoller(new SHEventHandler(tailer, filter));
                closeWhenRunEnds = false;
            } else {
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

    @Override
    public ChronicleChannel asInternalChannel(ChronicleContext context, ChronicleChannelCfg<?> channelCfg) {
        return new SubscribeQueueChannel(channelCfg, this, newQueue(context, subscribe, syncMode, sourceId));
    }

    static class SHEventHandler extends SimpleCloseable implements EventPoller {
        private final ExcerptTailer tailer;
        private final Predicate<Wire> filter;

        SHEventHandler(ExcerptTailer tailer, Predicate<Wire> filter) {
            this.tailer = tailer;
            this.filter = filter;
        }

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
     * @param sourceId the sourceId of the subscribe queue
     * @return this
     */
    public SubscribeHandler subscribeSourceId(int sourceId) {
        this.sourceId = sourceId;
        return this;
    }

    /**
     * @param subscriptionIndexController controls where the subscriptions will start to read from, by allowing the caller to
     *                                    {@link net.openhft.chronicle.queue.ExcerptTailer#moveToIndex(long) to control the first read location
     */
    public SubscribeHandler subscriptionIndexController(Consumer<ExcerptTailer> subscriptionIndexController) {
        this.subscriptionIndexController = subscriptionIndexController;
        return this;
    }
}
