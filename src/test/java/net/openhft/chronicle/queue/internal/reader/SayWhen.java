/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.reader;

import net.openhft.chronicle.wire.LongConversion;
import net.openhft.chronicle.wire.MicroTimestampLongConverter;

public interface SayWhen {
    void sayWhen(@LongConversion(MicroTimestampLongConverter.class) long timestamp, String message);
}
