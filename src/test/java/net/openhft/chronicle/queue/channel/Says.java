package net.openhft.chronicle.queue.channel;

import net.openhft.chronicle.core.io.Syncable;

public interface Says extends Syncable {
    void say(String say);
}
