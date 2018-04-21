package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.impl.WireStore;
import net.openhft.chronicle.wire.Wires;

public final class QueueInspector {
    private static final int NO_CURRENT_WRITER = Integer.MIN_VALUE;

    private final SingleChronicleQueue queue;

    public QueueInspector(final SingleChronicleQueue queue) {
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