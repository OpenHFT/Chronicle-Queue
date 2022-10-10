package run.chronicle.queue.channel.simple;

import net.openhft.chronicle.core.io.Syncable;

public interface Says extends Syncable {
    void say (String say);
}
