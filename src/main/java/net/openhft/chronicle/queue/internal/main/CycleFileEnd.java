/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.main;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.Wires;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/** Reads record boundaries without mapping or extending a cycle file. */
final class CycleFileEnd {
    static final long NO_EOF = -1;
    static final long NONZERO_TAIL = -2;
    private static final int MAX_HEADER = 1024 * 1024;

    private CycleFileEnd() {
    }

    // Non-negative: retained length; otherwise NO_EOF or a distinct NONZERO_TAIL refusal.
    static long find(FileChannel channel) throws IOException {
        long size = channel.size();
        if (size < Integer.BYTES)
            return -1;
        long position = headerEnd(channel);
        CycleHeaderReader headers = new CycleHeaderReader(channel, size);
        while (position <= size - Integer.BYTES) {
            position = (position + 3L) & ~3L;
            if (position > size - Integer.BYTES)
                return -1;
            int header = headers.integer(position);
            if (header == Wires.END_OF_DATA)
                return retainedEnd(channel, position + Integer.BYTES, size);
            if (!Wires.isReady(header))
                return -1;
            long end = position + Integer.BYTES + Wires.lengthOf(header);
            if (end > size)
                throw new StreamCorruptedException("Record extends beyond cycle file at " + position);
            position = end;
        }
        return -1;
    }

    /** Validate only the bounded store header, including files excluded as newest. */
    static long headerEnd(FileChannel channel) throws IOException {
        long size = channel.size();
        if (size < Integer.BYTES)
            throw new StreamCorruptedException("Expected a complete Queue store header");
        CycleHeaderReader headers = new CycleHeaderReader(channel, size);
        int header = headers.integer(0);
        if (!Wires.isReadyMetaData(header))
            throw new StreamCorruptedException("Expected a complete Queue store header");
        int length = Wires.lengthOf(header);
        if (length > MAX_HEADER || length > size - Integer.BYTES)
            throw new StreamCorruptedException("Queue store header exceeds file or inspection bound");
        ByteBuffer payload = ByteBuffer.allocate(length);
        read(channel, payload, Integer.BYTES);
        format(payload.array()); // Refuses every format except explicit dataFormat 1.
        return Integer.BYTES + (long) length;
    }

    private static long retainedEnd(FileChannel channel, long end, long size) throws IOException {
        if (size - end < 4)
            return -1;
        ByteBuffer annotation = ByteBuffer.allocate(4);
        read(channel, annotation, end);
        byte[] marker = annotation.array();
        if ((marker[0] & 255) != 0xc3 || marker[1] != 'E' || marker[2] != 'O' || marker[3] != 'F')
            return -1;
        end += 4;
        ByteBuffer tail = ByteBuffer.allocate(64 * 1024);
        long position = end;
        while (position < size) {
            tail.clear().limit((int) Math.min(tail.capacity(), size - position));
            int count = tail.remaining();
            read(channel, tail, position);
            for (int i = 0; i < count; i++)
                if (tail.get(i) != 0)
                    return NONZERO_TAIL;
            position += count;
        }
        return end;
    }

    private static int format(byte[] payload) throws IOException {
        Bytes<?> bytes = Bytes.wrapForRead(payload);
        try {
            BinaryWire wire = new BinaryWire(bytes);
            StringBuilder name = new StringBuilder();
            ValueIn value = wire.readEventName(name);
            if (!"header".contentEquals(name))
                throw new StreamCorruptedException("Not a Queue store header");
            StringBuilder type = new StringBuilder();
            value.typePrefix(type, StringBuilder::append);
            if (!"SCQStore".contentEquals(type))
                throw new StreamCorruptedException("Unsupported Queue store type: " + type);
            int[] format = {-1};
            value.marshallable(w -> {
                ValueIn position = w.read("writePosition");
                long start = w.bytes().readPosition();
                w.consumePadding();
                int code = w.bytes().readUnsignedByte();
                w.bytes().readPosition(start);
                if (code == net.openhft.chronicle.wire.BinaryWireCode.I64_ARRAY) {
                    try (net.openhft.chronicle.bytes.ref.BinaryTwoLongReference ref =
                                 new net.openhft.chronicle.bytes.ref.BinaryTwoLongReference()) {
                        position.int128(ref);
                    }
                } else {
                    position.int64();
                }
                indexing(w.read("indexing"));
                format[0] = w.read("dataFormat").int32();
            });
            if (format[0] != 1)
                throw new StreamCorruptedException("Only explicit Queue dataFormat 1 is supported");
            return format[0];
        } catch (RuntimeException e) {
            throw new StreamCorruptedException("Cannot decode Queue store header: " + e);
        } finally {
            bytes.releaseLast();
        }
    }

    private static void indexing(ValueIn value) {
        StringBuilder type = new StringBuilder();
        value.typePrefix(type, StringBuilder::append);
        if (!"SCQSIndexing".contentEquals(type))
            throw new IllegalArgumentException("Unsupported indexing type: " + type);
        if (!value.marshallable(w -> {
            long count = requiredLong(w, "indexCount"), spacing = requiredLong(w, "indexSpacing");
            if (count <= 0 || count > Integer.MAX_VALUE || (count & (count - 1)) != 0
                    || spacing <= 0 || spacing > Integer.MAX_VALUE || (spacing & (spacing - 1)) != 0)
                throw new IllegalArgumentException("Invalid indexing count/spacing");
            if (requiredLong(w, "index2Index") < 0 || requiredLong(w, "lastIndex") < 0)
                throw new IllegalArgumentException("Negative indexing position");
        }))
            throw new IllegalArgumentException("Missing indexing object");
    }

    private static long requiredLong(net.openhft.chronicle.wire.WireIn wire, String name) {
        ValueIn value = wire.read(name);
        if (!value.isPresent()) throw new IllegalArgumentException("Missing indexing field: " + name);
        return value.int64();
    }

    private static void read(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        while (buffer.hasRemaining()) {
            int count = channel.read(buffer, position);
            if (count <= 0)
                throw new StreamCorruptedException("File changed or ended during inspection at " + position);
            position += count;
        }
    }
}
