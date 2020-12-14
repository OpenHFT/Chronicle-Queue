package net.openhft.chronicle.queue.util;

import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.internal.InternalPretouchHandler;
import org.jetbrains.annotations.NotNull;

public final class PretouchUtil {

    private PretouchUtil() {}

    public static EventHandler createEventHandler(@NotNull final SingleChronicleQueue queue) {
        return new InternalPretouchHandler(queue);
    }
}