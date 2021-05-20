package net.openhft.chronicle.queue.internal.reader;

import net.openhft.chronicle.wire.LongConversion;
import net.openhft.chronicle.wire.MicroTimestampLongConverter;

public interface SayWhen {
    void sayWhen(@LongConversion(MicroTimestampLongConverter.class) long timestamp, String message);
}
