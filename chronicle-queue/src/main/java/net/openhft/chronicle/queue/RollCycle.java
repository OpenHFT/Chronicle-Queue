package net.openhft.chronicle.queue;

import java.util.Objects;

/**
 * Created by peter.lawrey on 19/09/2015.
 */
public interface RollCycle {
    static RollCycles forLength(int length) {
        for (int i = RollCycles.VALUES.length - 1; i >= 0; i--) {
            if (RollCycles.VALUES[i].length == length) {
                return RollCycles.VALUES[i];
            }
        }

        throw new IllegalArgumentException("Unknown value for CycleLength (" + length + ")");
    }

    static RollCycles forFormat(String format) {
        for (int i = RollCycles.VALUES.length - 1; i >= 0; i--) {
            if (Objects.equals(RollCycles.VALUES[i].format, format) || RollCycles.VALUES[i].format.equals(format)) {
                return RollCycles.VALUES[i];
            }
        }

        throw new IllegalArgumentException("Unknown value for CycleFormat (" + format + ")");
    }

    String format();

    int length();
}
