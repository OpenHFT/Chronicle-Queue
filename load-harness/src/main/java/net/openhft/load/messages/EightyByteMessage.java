package net.openhft.load.messages;

import net.openhft.chronicle.wire.SelfDescribingMarshallable;

public final class EightyByteMessage extends SelfDescribingMarshallable {
    private static final long UNSET_VALUE = Long.MAX_VALUE;
    public long batchStartNanos = 0L;
    public long publishNanos = 0L;
    public long batchStartMillis = 0L;
    public long stagesToPublishBitMask = 0L;
    public long t0 = UNSET_VALUE;
    public long t1 = UNSET_VALUE;
    public long t2 = UNSET_VALUE;
    public long t3 = UNSET_VALUE;
    public long t4 = UNSET_VALUE;
    public long t5 = UNSET_VALUE;
    public long t6 = UNSET_VALUE;

    public static boolean isSet(final long value) {
        return value != UNSET_VALUE;
    }
}
