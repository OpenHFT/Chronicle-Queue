/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;

import static org.junit.Assert.assertEquals;

final class QueuePerfTestSupport {
    private QueuePerfTestSupport() {
    }

    static void writeMany(Bytes<?> bytes, int size) {
        for (int i = 0; i < size; i += 32) {
            bytes.writeInt(i); // 4 bytes
            bytes.writeFloat(i); // 4 bytes
            bytes.writeLong(i); // 8 bytes
            bytes.writeDouble(i); // 8 bytes
            bytes.writeUtf8("Hello!!"); // 8 bytes
        }
    }

    static void readMany(Bytes<?> bytes, int size) {
        for (int i = 0; i < size; i += 32) {
            // blackholes to avoid code elimination.
            int s32 = bytes.readInt(); // 4 bytes
            float f32 = bytes.readFloat(); // 4 bytes
            long s64 = bytes.readLong(); // 8 bytes
            double f64 = bytes.readDouble(); // 8 bytes
            String s = bytes.readUtf8(); // 8 bytes
            assertEquals("Hello!!", s);
        }
    }

    interface TestWriter<T> {
        void writeTo(T t);
    }

    interface TestReader<T> {
        void readFrom(T t);
    }
}
