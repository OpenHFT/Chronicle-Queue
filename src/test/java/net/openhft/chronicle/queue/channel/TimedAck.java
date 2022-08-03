package net.openhft.chronicle.queue.channel;

import net.openhft.chronicle.wire.converter.NanoTime;

public interface TimedAck {
    TimedAck ack(@NanoTime long time);

    void flush();
}
