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

@SuppressWarnings("deprecation")
public class PublishQueueChannel implements ChronicleChannel {
    private final ChronicleChannelCfg<?> channelCfg;
    private final AbstractHandler<?> publishHandler;
    private final ChannelHeader headerOut;
    private final ChronicleQueue publishQueue;
    private final ExcerptTailer tailer;

    public PublishQueueChannel(ChronicleChannelCfg<?> channelCfg, AbstractHandler<?> publishHandler, ChronicleQueue publishQueue) {
        this.channelCfg = channelCfg;
        this.publishHandler = publishHandler;
        this.headerOut = publishHandler.responseHeader(null);
        this.publishQueue = publishQueue;
        tailer = publishQueue.createTailer();
    }

    @Override
    public ChronicleChannelCfg<?> channelCfg() {
        return channelCfg;
    }

    @Override
    public ChannelHeader headerOut() {
        return headerOut;
    }

    @Override
    public ChannelHeader headerIn() {
        return publishHandler;
    }

    @Override
    public void close() {
        Closeable.closeQuietly(
                tailer,
                publishQueue);
    }

    @Override
    public boolean isClosed() {
        return publishQueue.isClosed();
    }

    @Override
    public DocumentContext readingDocument() {
        return NoDocumentContext.INSTANCE;
    }

    @Override
    public DocumentContext writingDocument(boolean metaData) throws UnrecoverableTimeoutException {
        return acquireThreadLocalAppender(publishQueue).writingDocument(metaData);
    }

    @Override
    public DocumentContext acquireWritingDocument(boolean metaData) throws UnrecoverableTimeoutException {
        return acquireThreadLocalAppender(publishQueue).acquireWritingDocument(metaData);
    }

    @Override
    public void testMessage(long now) {
        try (DocumentContext dc = writingDocument(true)) {
            dc.wire().write("testMessage").writeLong(NanoTime.INSTANCE, now);
        }
    }

    @Override
    public long lastTestMessage() {
        throw new UnsupportedOperationException();
    }
}
