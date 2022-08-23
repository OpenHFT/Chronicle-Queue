package net.openhft.chronicle.queue.channel;

import net.openhft.chronicle.bytes.MethodId;

interface Echoing {
    @MethodId('e')
    void echo(DummyData data);
}
