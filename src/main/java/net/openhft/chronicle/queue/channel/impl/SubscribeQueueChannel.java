package net.openhft.chronicle.queue.channel.impl;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.NoDocumentContext;
import net.openhft.chronicle.wire.UnrecoverableTimeoutException;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.channel.AbstractHandler;
import net.openhft.chronicle.wire.channel.ChannelHeader;
import net.openhft.chronicle.wire.channel.ChronicleChannel;
import net.openhft.chronicle.wire.channel.ChronicleChannelCfg;
import net.openhft.chronicle.wire.converter.NanoTime;

import java.util.function.Function;

public class SubscribeQueueChannel implements ChronicleChannel {
    private final ChronicleChannelCfg channelCfg;
    private final AbstractHandler pipeHandler;
    private final ChannelHeader headerOut;
    private final ChronicleQueue subscribeQueue;
    private final ExcerptTailer tailer;
    private long lastTestMessage;

    public SubscribeQueueChannel(ChronicleChannelCfg channelCfg, AbstractHandler pipeHandler, ChronicleQueue subscribeQueue) {
        this.channelCfg = channelCfg;
        this.pipeHandler = pipeHandler;
        this.headerOut = pipeHandler.responseHeader(null);
        this.subscribeQueue = subscribeQueue;
        tailer = subscribeQueue.createTailer();
    }

    @Override
    public ChronicleChannelCfg channelCfg() {
        return channelCfg;
    }

    @Override
    public ChannelHeader headerOut() {
        return headerOut;
    }

    @Override
    public ChannelHeader headerIn() {
        return pipeHandler;
    }

    @Override
    public void close() {
        Closeable.closeQuietly(
                tailer,
                subscribeQueue);
    }

    @Override
    public boolean isClosed() {
        return subscribeQueue.isClosed();
    }

    @Override
    public DocumentContext readingDocument() {
        final DocumentContext dc = tailer.readingDocument(true);
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

    @Override
    public DocumentContext writingDocument(boolean metaData) throws UnrecoverableTimeoutException {
        return NoDocumentContext.INSTANCE;
    }

    @Override
    public DocumentContext acquireWritingDocument(boolean metaData) throws UnrecoverableTimeoutException {
        return NoDocumentContext.INSTANCE;
    }

    @Override
    public void testMessage(long now) {
    }

    @Override
    public long lastTestMessage() {
        return lastTestMessage;
    }
}
