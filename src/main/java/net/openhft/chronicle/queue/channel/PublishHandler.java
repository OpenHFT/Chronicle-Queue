package net.openhft.chronicle.queue.channel;

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.bytes.SyncMode;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.channel.impl.PublishQueueChannel;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.channel.AbstractHandler;
import net.openhft.chronicle.wire.channel.ChronicleChannel;
import net.openhft.chronicle.wire.channel.ChronicleChannelCfg;
import net.openhft.chronicle.wire.channel.ChronicleContext;

import static net.openhft.chronicle.queue.channel.PipeHandler.newQueue;

public class PublishHandler extends AbstractHandler<PublishHandler> {
    private String publish;
    private SyncMode syncMode;
    private int publishSourceId = 0;

    static void copyFromChannelToQueue(ChronicleChannel channel, Pauser pauser, ChronicleQueue publishQueue, SyncMode syncMode) {
        try (ChronicleQueue publishQ = publishQueue;
             ExcerptAppender appender = publishQ.acquireAppender()) {
            appender.singleThreadedCheckDisabled(true);  // assume we are thread safe

            boolean needsSync = false;
            while (!channel.isClosed()) {
                try (DocumentContext dc = channel.readingDocument()) {
                    pauser.unpause();

                    if (!dc.isPresent()) {
                        if (needsSync) {
                            syncAppender(appender, syncMode);
                            needsSync = false;
                        }
                        continue;
                    }
                    if (dc.isMetaData()) {
                        // read message
                        continue;
                    }

                    try (DocumentContext dc2 = appender.writingDocument()) {
                        dc.wire().copyTo(dc2.wire());
                        needsSync = syncMode == SyncMode.SYNC || syncMode == SyncMode.ASYNC;
                    }
                }
            }

        } finally {
            Thread.currentThread().setName("connections");
        }
    }

    private static void syncAppender(ExcerptAppender appender, SyncMode syncMode) {
        if (syncMode == SyncMode.SYNC) {
            try (DocumentContext dc2 = appender.writingDocument()) {
                dc2.wire().write("sync").text("");
            }
        }
        appender.sync();
    }

    public String publish() {
        return publish;
    }

    public PublishHandler publish(String publish) {
        this.publish = publish;
        return this;
    }

    public SyncMode syncMode() {
        return syncMode;
    }

    public PublishHandler syncMode(SyncMode syncMode) {
        this.syncMode = syncMode;
        return this;
    }

    public int publishSourceId() {
        return publishSourceId;
    }

    public PublishHandler publishSourceId(int publishSourceId) {
        this.publishSourceId = publishSourceId;
        return this;
    }

    @Override
    public void run(ChronicleContext context, ChronicleChannel channel) {
        Pauser pauser = Pauser.balanced();

        Thread.currentThread().setName("publish~reader");
        try (AffinityLock lock = context.affinityLock()) {
            copyFromChannelToQueue(channel, pauser, newQueue(context, publish, syncMode, publishSourceId), syncMode);
        }
    }

    @Override
    public ChronicleChannel asInternalChannel(ChronicleContext context, ChronicleChannelCfg channelCfg) {
        return new PublishQueueChannel(channelCfg, this, newQueue(context, publish, syncMode, publishSourceId));
    }
}
