package net.openhft.chronicle.queue.reader;

@FunctionalInterface
public interface Say {
    void say(final String msg);
}
