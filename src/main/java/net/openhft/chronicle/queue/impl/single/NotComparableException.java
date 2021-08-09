package net.openhft.chronicle.queue.impl.single;

/**
 * Thrown by a binary search comparator when the value we're searching on is not present in the current
 * entry.
 * <p>
 * The assumption is that this occurs rarely. If it does, it will significantly reduce the performance
 * of the binary search.
 */
public final class NotComparableException extends RuntimeException {

    public static final NotComparableException INSTANCE = new NotComparableException();

    private NotComparableException() {
    }
}
