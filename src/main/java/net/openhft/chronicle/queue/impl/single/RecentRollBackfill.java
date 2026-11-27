/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.wire.Wire;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static net.openhft.chronicle.wire.Wires.*;

/** One source-side replay from a connected sink, starting at sequence zero of a recent roll. */
public final class RecentRollBackfill {
    public static final String PROPERTY = "chronicle.queue.recoverSealedRollsFromSink";
    private final SingleChronicleQueue queue;
    private final StoreAppender appender;
    private final long firstIndex;
    private long previous = -1;
    private boolean failed;

    RecentRollBackfill(SingleChronicleQueue queue, StoreAppender appender) {
        this.queue = queue;
        this.appender = appender;
        int last = queue.lastCycle();
        int first = queue.firstCycle();
        firstIndex = first > last ? 0 : queue.rollCycle().toIndex(
                last > first && queue.cycleFileExists(last - 1) ? last - 1 : last, 0);
    }

    /** First index requested from this sink, including the prefix that it must corroborate. */
    public long firstIndex() {
        return firstIndex;
    }

    /** Verify one complete overlapping record or append the exact next record from this sink. */
    public void write(long index, BytesStore<?, ?> payload) {
        appender.throwExceptionIfClosed();
        appender.checkAppendLock();
        WriteLock lock = queue.writeLock();
        lock.lock();
        try {
            appender.checkAppendLock();
            if (failed)
                throw new IllegalStateException("This recovery connection has failed");
            RollCycle roll = queue.rollCycle();
            int cycle = roll.toCycle(index);
            long seq = roll.toSequenceNumber(index);
            boolean newRoll = previous < 0 || roll.toCycle(previous) != cycle;
            if (index < firstIndex || index <= previous || (newRoll ? seq != 0 : index != previous + 1)
                    || seq >= roll.maxMessagesPerCycle() || (long) cycle < (long) queue.lastPublishedCycle() - 1)
                throw new IllegalArgumentException("Unexpected sink replay index 0x" + Long.toHexString(index));
            try (SingleChronicleQueueStore store = queue.storeForCycle(cycle, queue.epoch(), false, null)) {
                if (store == null || !applyToExisting(store, seq, index, payload))
                    appender.writeBytesInternal(index, payload);
            }
            previous = index;
        } catch (IOException e) {
            failed = true;
            throw new IORuntimeException(e);
        } catch (RuntimeException | Error e) {
            failed = true;
            throw e;
        } finally {
            lock.unlock();
        }
    }

    private boolean applyToExisting(SingleChronicleQueueStore store, long seq, long index, BytesStore<?, ?> payload) throws IOException {
        if (store.dataVersion() <= 0)
            throw reject(index, "unaligned store");
        try (MappedBytes bytes = store.bytes()) {
            Wire wire = queue.wireType().apply(bytes);
            wire.usePadding(true);
            StoreAppender.WireExcerptContext context = new StoreAppender.WireExcerptContext(wire, appender.timeoutMS());
            long last = store.lastPublishedSequenceNumber(wire);
            if (seq <= last) {
                if (store.moveToIndexForRead(context, seq) != ScanResult.FOUND || !matches(bytes, bytes.readPosition(), payload))
                    throw new IllegalArgumentException("Sink differs from published source record at 0x" + Long.toHexString(index));
                return true;
            }
            if (seq != last + 1)
                throw reject(index, "gap in the source roll");
            long published = store.writePosition();
            long pos = skipMetadata(bytes, after(bytes, published));
            int header = bytes.readVolatileInt(pos);
            if (header == NOT_INITIALIZED)
                return false;
            // A process can die after the seal advanced and before the write position was published.
            if (isReadyData(header) && matches(bytes, pos, payload)) {
                long seal = skipMetadata(bytes, after(bytes, pos));
                if (bytes.readVolatileInt(seal) != END_OF_DATA)
                    throw reject(index, "unpublished record without a verified successor seal");
                store.indexing.prepareSealedIndex(wire, seq, seal);
                publish(store, context, bytes, seq, pos);
                return true;
            }
            if (header != END_OF_DATA)
                throw reject(index, "incomplete or unverified terminal record");
            long length = payload.readRemaining();
            if (length <= 0 || length > LENGTH_MASK)
                throw reject(index, "invalid payload length");
            pos = store.indexing.prepareSealedIndex(wire, seq, pos);
            bytes.writePosition(pos + SPB_HEADER_SIZE);
            bytes.write(payload, payload.readPosition(), length);
            advanceSeal(bytes, pos, bytes.writePosition(), false);
            publish(store, context, bytes, seq, pos);
            return true;
        }
    }

    private static boolean matches(MappedBytes bytes, long pos, BytesStore<?, ?> payload) {
        int header = bytes.readVolatileInt(pos);
        if (!isReadyData(header) || lengthOf(header) != payload.readRemaining())
            return false;
        bytes.readPositionUnlimited(pos + SPB_HEADER_SIZE);
        return bytes.equalBytes(payload, lengthOf(header));
    }

    private static long after(MappedBytes bytes, long pos) {
        long end = pos + SPB_HEADER_SIZE + lengthOf(bytes.readVolatileInt(pos));
        return end + BytesUtil.padOffset(end);
    }

    private static long skipMetadata(MappedBytes bytes, long pos) {
        while (isReadyMetaData(bytes.readVolatileInt(pos)))
            pos = after(bytes, pos);
        return pos;
    }

    static long advanceSeal(MappedBytes bytes, long seal, long end, boolean metadata) throws IOException {
        long next = end + BytesUtil.padOffset(end);
        bytes.writeInt(next, END_OF_DATA);
        force(bytes, seal, next + SPB_HEADER_SIZE);
        int length = Math.toIntExact(end - seal - SPB_HEADER_SIZE);
        if (!bytes.compareAndSwapInt(seal, END_OF_DATA, metadata ? length | META_DATA : length))
            throw new IllegalStateException("EOF changed under the queue write lock");
        force(bytes, seal, seal + SPB_HEADER_SIZE);
        return next;
    }

    private static void publish(SingleChronicleQueueStore store, StoreAppender.WireExcerptContext context, MappedBytes bytes,
                                long seq, long pos) throws IOException {
        store.setPositionForSequenceNumber(context, seq, pos);
        store.writePosition(pos);
        // The sparse index, encoded sequence and store header can live on different pages.
        force(bytes, 0, pos + SPB_HEADER_SIZE);
    }

    static void force(MappedBytes bytes, long from, long to) throws IOException {
        FileChannel channel = bytes.mappedFile().raf().getChannel();
        while (from < to) {
            long length = Math.min(to - from, 64L << 20);
            MappedByteBuffer view = channel.map(FileChannel.MapMode.READ_WRITE, from, length);
            try {
                view.force();
            } finally {
                IOTools.clean(view);
            }
            from += length;
        }
    }

    private static ExactWriteBoundaryException reject(long index, String reason) {
        return new ExactWriteBoundaryException("Reverse recovery rejected at 0x" + Long.toHexString(index) + ": " + reason);
    }
}
