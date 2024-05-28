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

    /**
     * Get the sequence number of the last entry present in the cycle.
     * <p>
     * Note: If you're not holding the write lock when you call this and there are concurrent writers,
     * the value may be stale by the time it's returned. If you're holding the write lock it is guaranteed
     * to be accurate.
     *
     * @param ex An {@link ExcerptContext} used to scan the roll cycle if necssary
     * @return the sequence of the last excerpt in the cycle
     * @throws StreamCorruptedException if the index is corrupt
     */
    long lastSequenceNumber(ExcerptContext ex) throws StreamCorruptedException;

    int linearScanCount();

    int linearScanByPositionCount();
}
