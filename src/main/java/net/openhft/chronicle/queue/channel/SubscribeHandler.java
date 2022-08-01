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
import net.openhft.chronicle.wire.channel.*;

import static net.openhft.chronicle.queue.channel.PipeHandler.newQueue;

public class SubscribeHandler extends AbstractHandler<SubscribeHandler> {
    private String subscribe;
    private transient boolean closeWhenRunEnds = true;

    private SyncMode syncMode;

    static void queueTailer(Pauser pauser, ChronicleChannel channel, ChronicleQueue subscribeQueue) {
        try (ChronicleQueue subscribeQ = subscribeQueue; // leave here so it gets closed
             ExcerptTailer tailer = subscribeQ.createTailer()) {
            while (!channel.isClosing()) {
                if (copyOneMessage(channel, tailer))
                    pauser.reset();
                else
                    pauser.pause();
            }
        } catch (Exception e) {
            Thread.yield();
            if (channel.isClosing())
                return;
            throw e;
        }
    }

    static boolean copyOneMessage(ChronicleChannel channel, ExcerptTailer tailer) {
        try (DocumentContext dc = tailer.readingDocument()) {
            if (!dc.isPresent()) {
                return false;
            }
            if (dc.isMetaData()) {
                return false;
            }

            final long dataBuffered;
            try (DocumentContext dc2 = channel.writingDocument()) {
                dc.wire().copyTo(dc2.wire());

                dataBuffered = dc2.wire().bytes().writePosition();
            }
            // wait for it to drain
            return dataBuffered < 32 << 10;
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

    @Override
    public void run(ChronicleContext context, ChronicleChannel channel) {
        Pauser pauser = Pauser.balanced();

        final ExcerptTailer tailer;

        ChronicleQueue subscribeQ = newQueue(context, subscribe, syncMode);
        InternalChronicleChannel icc = (InternalChronicleChannel) channel;
        if (icc.supportsEventPoller()) {
            tailer = subscribeQ.createTailer();
            icc.eventPoller(new SHEventHandler(tailer));
            closeWhenRunEnds = false;
        } else {
            try (AffinityLock lock = context.affinityLock()) {
                queueTailer(pauser, channel, newQueue(context, subscribe, syncMode));
            }
            closeWhenRunEnds = true;
        }
    }

    @Override
    public boolean closeWhenRunEnds() {
        return closeWhenRunEnds;
    }

    @Override
    public ChronicleChannel asInternalChannel(ChronicleContext context, ChronicleChannelCfg channelCfg) {
        return new SubscribeQueueChannel(channelCfg, this, newQueue(context, subscribe, syncMode));
    }

    static class SHEventHandler extends SimpleCloseable implements EventPoller {
        private final ExcerptTailer tailer;

        SHEventHandler(ExcerptTailer tailer) {
            this.tailer = tailer;
        }

        @Override
        public boolean onPoll(ChronicleChannel channel) {
            boolean wrote = false;
            while (copyOneMessage(channel, tailer))
                wrote = true;
            return wrote;
        }

        @Override
        protected void performClose() {
            super.performClose();
            Closeable.closeQuietly(tailer);
        }
    }
}