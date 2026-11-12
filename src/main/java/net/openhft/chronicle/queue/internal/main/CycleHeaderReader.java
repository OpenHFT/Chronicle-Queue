/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.main;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

/** Bounded read-ahead for an externally quiesced file; never maps or extends it. */
final class CycleHeaderReader {
    private final FileChannel channel;
    private final long size;
    private final ByteBuffer buffer = ByteBuffer.allocate(64 * 1024).order(ByteOrder.LITTLE_ENDIAN);
    private long start = -1;

    CycleHeaderReader(FileChannel channel, long size) {
        this.channel = channel;
        this.size = size;
        buffer.limit(0);
    }

    int integer(long position) throws IOException {
        if (position < 0 || position > size - Integer.BYTES)
            throw new StreamCorruptedException("Header outside cycle file: " + position);
        if (start < 0 || position < start || position - start > buffer.limit() - Integer.BYTES) {
            start = position;
            buffer.clear().limit((int) Math.min(buffer.capacity(), size - position));
            while (buffer.hasRemaining()) {
                int count = channel.read(buffer, start + buffer.position());
                if (count <= 0)
                    throw new StreamCorruptedException("File changed during header read at " + position);
            }
            buffer.flip();
        }
        return buffer.getInt((int) (position - start));
    }
}
