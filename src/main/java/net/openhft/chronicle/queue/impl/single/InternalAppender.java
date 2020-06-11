package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.queue.ExcerptAppender;

/**
 * please don't use this interface as its an internal implementation.
 */
public interface InternalAppender extends ExcerptAppender {
    void writeBytes(long index, BytesStore bytes);
}
