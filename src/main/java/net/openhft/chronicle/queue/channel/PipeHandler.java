package net.openhft.chronicle.queue.channel;


import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.bytes.SyncMode;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.ClosedIORuntimeException;
import net.openhft.chronicle.core.io.SimpleCloseable;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.channel.impl.QueuesChannel;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.channel.*;
import net.openhft.chronicle.wire.channel.impl.BufferedChronicleChannel;

import java.io.File;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static net.openhft.chronicle.queue.channel.PublishHandler.copyFromChannelToQueue;

public class PipeHandler extends AbstractHandler<PipeHandler> {
    private String publish;
    private String subscribe;
    private SyncMode syncMode;
    private transient Thread tailerThread;

    private Predicate<Wire> filter = null;

    private int publishSourceId = 0;

    private int subscribeSourceId = 0;
    private Consumer<ExcerptTailer> subscriptionIndexController = SubscribeHandler.NO_OP;


    public PipeHandler() {
    }

    static ChronicleQueue newQueue(ChronicleContext context, String queueName, SyncMode syncMode, int sourceId) {
        final File path = context.toFile(queueName);
        return ChronicleQueue.singleBuilder(path).blockSize(OS.isSparseFileSupported() ? 512L << 30 : 64L << 20).sourceId(sourceId).syncMode(syncMode).build();
    }

    public String publish() {
        return publish;
    }

    public PipeHandler publish(String publish) {
        this.publish = publish;
        return this;
    }

    public String subscribe() {
        return subscribe;
    }

    public PipeHandler subscribe(String subscribe) {
        this.subscribe = subscribe;
        return this;
    }

    public SyncMode syncMode() {
        return syncMode;
    }

    public PipeHandler syncMode(SyncMode syncMode) {
        this.syncMode = syncMode;
        return this;
    }

    public Predicate<Wire> filter() {
        return filter;
    }

    public PipeHandler filter(Predicate<Wire> filter) {
        this.filter = filter;
        return this;
    }

    public int publishSourceId() {
        return publishSourceId;
    }

    public PipeHandler publishSourceId(int publishSourceId) {
        this.publishSourceId = publishSourceId;
        return this;
    }

    public PipeHandler subscribeSourceId(int subscribeSourceId) {
        this.subscribeSourceId = subscribeSourceId;
        return this;
    }


    @Override
    public void run(ChronicleContext context, ChronicleChannel channel) {
        Pauser pauser = Pauser.balanced();

        try (ChronicleQueue subscribeQ = newQueue(context, subscribe, syncMode, subscribeSourceId)) {
            final ExcerptTailer tailer;

            if (channel instanceof BufferedChronicleChannel) {
                BufferedChronicleChannel bc = (BufferedChronicleChannel) channel;
                tailer = subscribeQ.createTailer();
                tailer.singleThreadedCheckDisabled(true);  // assume we are thread safe
                subscriptionIndexController.accept(tailer);
                bc.eventPoller(new PHEventPoller(tailer, filter));
            } else {
                tailerThread = new Thread(() -> {
                    try (AffinityLock lock = context.affinityLock()) {
                        SubscribeHandler.queueTailer(pauser, channel, subscribeQ, filter, subscriptionIndexController);
                    } catch (ClosedIORuntimeException e) {
                        Jvm.warn().on(PipeHandler.class, e.toString());
                    } catch (Throwable t) {
                        Jvm.warn().on(PipeHandler.class, t);
                    }
                }, "pipe~tailer");
                tailerThread.setDaemon(true);
                tailerThread.start();
            }

            Thread.currentThread().setName("pipe~reader");
            try (AffinityLock lock = context.affinityLock()) {
                copyFromChannelToQueue(channel, pauser, newQueue(context, publish, syncMode, publishSourceId), syncMode);
            } finally {
                if (tailerThread != null) tailerThread.interrupt();
            }
        }
    }

    @Override
    public ChronicleChannel asInternalChannel(ChronicleContext context, ChronicleChannelCfg channelCfg) {
        return new QueuesChannel(channelCfg, this, newQueue(context, publish, syncMode, publishSourceId), newQueue(context, subscribe, syncMode, 0));
    }

    static class PHEventPoller extends SimpleCloseable implements EventPoller {
        private final ExcerptTailer tailer;
        private final Predicate<Wire> filter;

        public PHEventPoller(ExcerptTailer tailer, Predicate<Wire> filter) {
            this.tailer = tailer;
            this.filter = filter;
        }

        @Override
        public boolean onPoll(ChronicleChannel conn) {
            boolean wrote = false;
            while (SubscribeHandler.copyOneMessage(conn, tailer, filter)) wrote = true;
            return wrote;
        }

        @Override
        protected void performClose() {
            Closeable.closeQuietly(tailer, tailer.queue());
            super.performClose();
        }
    }

    /**
     * @param subscriptionIndexController controls where the subscriptions will start to read from, by allowing the caller to
     *                                    {@link net.openhft.chronicle.queue.ExcerptTailer#moveToIndex(long) to control the first read location
     */
    public PipeHandler subscriptionIndexController(Consumer<ExcerptTailer> subscriptionIndexController) {
        this.subscriptionIndexController = subscriptionIndexController;
        return this;
    }
}