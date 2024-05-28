package net.openhft.chronicle.queue.impl.single;

import static java.lang.String.format;

public class IllegalIndexException extends IllegalArgumentException {
    private static final long serialVersionUID = 0L;

    public IllegalIndexException(long providedIndex, long lastIndex) {
        super(format("Index provided is after the next index in the queue, provided index = %x, last index in queue = %x", providedIndex, lastIndex));
    }
}
