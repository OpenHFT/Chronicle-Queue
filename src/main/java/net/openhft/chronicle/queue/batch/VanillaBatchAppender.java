package net.openhft.chronicle.queue.batch;

/**
 * Created by Rob Austin
 */
public class VanillaBatchAppender implements BatchAppender {

    @Override
    public int rawMaxMessage() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public int rawMaxBytes() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public long rawAddress() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void write(final long sourceBytesAddress, final int sourceByteSize, final long endOfQueueAddress, final long numberOfMessagesInLastBatch) {
        throw new UnsupportedOperationException("todo");
    }
}
