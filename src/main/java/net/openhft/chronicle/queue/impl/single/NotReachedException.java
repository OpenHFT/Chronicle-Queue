package net.openhft.chronicle.queue.impl.single;

public class NotReachedException extends IllegalStateException {
    public NotReachedException(final String s) {
        super(s);
    }
}
