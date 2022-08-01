package net.openhft.chronicle.queue.channel.impl;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.UnrecoverableTimeoutException;
import net.openhft.chronicle.wire.channel.AbstractHandler;
import net.openhft.chronicle.wire.channel.ChannelHeader;
import net.openhft.chronicle.wire.channel.ChronicleChannel;
import net.openhft.chronicle.wire.channel.ChronicleChannelCfg;

import java.util.function.Function;

public class QueuesChannel implements ChronicleChannel {
    private final PublishQueueChannel publishQueueChannel;
    private final SubscribeQueueChannel subscribeQueueChannel;

    public QueuesChannel(ChronicleChannelCfg channelCfg, AbstractHandler handler, ChronicleQueue publishQueue, ChronicleQueue subscribeQueue) {
        publishQueueChannel = new PublishQueueChannel(channelCfg, handler, publishQueue);
        subscribeQueueChannel = new SubscribeQueueChannel(channelCfg, handler, subscribeQueue);
    }

    @Override
    public ChronicleChannelCfg channelCfg() {
        return publishQueueChannel.channelCfg();
    }

    @Override
    public ChannelHeader headerOut() {
        return publishQueueChannel.headerOut();
    }

    @Override
    public ChannelHeader headerIn() {
        return subscribeQueueChannel.headerIn();
    }

    @Override
    public ChannelHeader headerIn(Function<ChannelHeader, ChannelHeader> redirectFunction) {
        return subscribeQueueChannel.headerIn(redirectFunction);
    }

    @Override
    public void close() {
        Closeable.closeQuietly(
                publishQueueChannel,
                subscribeQueueChannel);
    }

    @Override
    public boolean isClosed() {
        return publishQueueChannel.isClosed() || subscribeQueueChannel.isClosed();
    }

    @Override
    public DocumentContext readingDocument() {
        return subscribeQueueChannel.readingDocument();
    }

    @Override
    public DocumentContext writingDocument(boolean metaData) throws UnrecoverableTimeoutException {
        return publishQueueChannel.writingDocument(metaData);
    }

    @Override
    public DocumentContext acquireWritingDocument(boolean metaData) throws UnrecoverableTimeoutException {
        return publishQueueChannel.acquireWritingDocument(metaData);
    }

    @Override
    public void testMessage(long now) {
        publishQueueChannel.testMessage(now);
    }

    @Override
    public long lastTestMessage() {
        return subscribeQueueChannel.lastTestMessage();
    }
}
