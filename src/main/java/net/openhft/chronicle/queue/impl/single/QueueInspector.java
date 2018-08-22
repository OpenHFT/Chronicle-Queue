package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.queue.impl.WireStore;
import net.openhft.chronicle.wire.Wires;

@Deprecated
// Not working in queue 5 since we no longer write the incomplete header
public final class QueueInspector {
    private static final int NO_CURRENT_WRITER = Integer.MIN_VALUE;

    private final RollingChronicleQueue queue;

    public QueueInspector(final RollingChronicleQueue queue) {
        this.queue = queue;
    }

    public static boolean isValidThreadId(final int writingThreadId) {
        return writingThreadId != NO_CURRENT_WRITER;
    }

    public int getWritingThreadId() {
        final WireStore wireStore = queue.storeForCycle(queue.cycle(), queue.epoch(), false);
        if (wireStore != null) {
            final long position = wireStore.writePosition();
            final int header = wireStore.bytes().readVolatileInt(position);
            if (Wires.isReady(header)) {
                final long nextHeaderPosition = position + Wires.lengthOf(header) + Wires.SPB_HEADER_SIZE;
                final int unfinishedHeader = wireStore.bytes().
                        readVolatileInt(nextHeaderPosition);
                if (Wires.isNotComplete(unfinishedHeader) && unfinishedHeader != 0) {
                    return Wires.extractTidFromHeader(unfinishedHeader);
                }
            }
        }
        return NO_CURRENT_WRITER;
    }
}