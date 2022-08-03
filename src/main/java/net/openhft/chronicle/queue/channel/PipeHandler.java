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
import net.openhft.chronicle.wire.channel.*;
import net.openhft.chronicle.wire.channel.impl.BufferedChronicleChannel;

import java.io.File;

import static net.openhft.chronicle.queue.channel.PublishHandler.copyFromChannelToQueue;

public class PipeHandler extends AbstractHandler<PipeHandler> {
    private String publish;
    private String subscribe;
    private SyncMode syncMode;
    private transient Thread tailerThread;

    public PipeHandler() {
    }

    static ChronicleQueue newQueue(ChronicleContext context, String subscribe, SyncMode syncMode) {
        final File path = context.toFile(subscribe);
        return ChronicleQueue.singleBuilder(path)
                .blockSize(OS.isSparseFileSupported() ? 512L << 30 : 64L << 20)
                .syncMode(syncMode)
                .build();
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

    @Override
    public void run(ChronicleContext context, ChronicleChannel channel) {
        Pauser pauser = Pauser.balanced();

        ChronicleQueue subscribeQ = newQueue(context, subscribe, syncMode);
        final ExcerptTailer tailer;

        if (channel instanceof BufferedChronicleChannel) {
            BufferedChronicleChannel bc = (BufferedChronicleChannel) channel;
            tailer = subscribeQ.createTailer();
            bc.eventPoller(new PHEventPoller(tailer));
        } else {
            tailerThread = new Thread(() -> {
                try (AffinityLock lock = context.affinityLock()) {
                    SubscribeHandler.queueTailer(pauser, channel, subscribeQ);
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
            copyFromChannelToQueue(channel, pauser, newQueue(context, publish, syncMode), syncMode);
        } finally {
            if (tailerThread != null)
                tailerThread.interrupt();
        }
    }

    @Override
    public ChronicleChannel asInternalChannel(ChronicleContext context, ChronicleChannelCfg channelCfg) {
        return new QueuesChannel(channelCfg, this, newQueue(context, publish, syncMode), newQueue(context, subscribe, syncMode));
    }

    static class PHEventPoller extends SimpleCloseable implements EventPoller {
        private final ExcerptTailer tailer;

        public PHEventPoller(ExcerptTailer tailer) {
            this.tailer = tailer;
        }

        @Override
        public boolean onPoll(ChronicleChannel conn) {
            boolean wrote = false;
            while (SubscribeHandler.copyOneMessage(conn, tailer))
                wrote = true;
            return wrote;
        }

        @Override
        protected void performClose() {
            Closeable.closeQuietly(tailer, tailer.queue());
            super.performClose();
        }
    }
}