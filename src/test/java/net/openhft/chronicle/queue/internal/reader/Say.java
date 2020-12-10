package net.openhft.chronicle.queue.internal.reader;

@FunctionalInterface
public interface Say {
    void say(final String msg);
}
