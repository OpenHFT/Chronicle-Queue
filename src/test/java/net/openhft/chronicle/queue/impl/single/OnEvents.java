package net.openhft.chronicle.queue.impl.single;

@FunctionalInterface
public interface OnEvents {
    void onEvent(final String event);
}
