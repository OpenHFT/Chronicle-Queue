package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.impl.ExcerptContext;

import java.io.StreamCorruptedException;

/**
 * Provides internal-only indexing functionality for {@link SingleChronicleQueue}. This interface will be expanded over
 * time to clarify the API contract of {@link SCQIndexing}.
 */
public interface Indexing {

    int indexCount();

    int indexSpacing();

    long nextEntryToBeIndexed();

    boolean indexable(long index);

    long lastSequenceNumber(ExcerptContext ec, boolean approximate) throws StreamCorruptedException;

    int linearScanCount();

    int linearScanByPositionCount();
}
