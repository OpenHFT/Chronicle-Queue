/*
 * Copyright 2016-2020 chronicle.software
 *
 * https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.StackTrace;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.ClosedIllegalStateException;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.util.StringUtils;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.single.MetaDataField;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static net.openhft.chronicle.core.util.Time.sleep;

public class SingleTableStore<T extends Metadata> extends AbstractCloseable implements TableStore<T> {
    public static final String SUFFIX = ".cq4t";
    private static final int EXCLUSIVE_LOCK_SIZE = 1;
    /**
     * We need to be able to acquire an "exclusive" lock while fine-grained long-running locks are being held.
     * For this reason the "exclusive" lock doesn't lock the whole file, but as long as everyone agrees
     * on what section constitutes an "exclusive" lock this should be fine.
     */
    private static final long EXCLUSIVE_LOCK_START = Long.MAX_VALUE - EXCLUSIVE_LOCK_SIZE;

    private static final long timeoutMS = Jvm.getLong("chronicle.table.store.timeoutMS", 10_000L);
    @NotNull
    private final WireType wireType;
    @NotNull
    private final T metadata;
    @NotNull
    private final MappedBytes mappedBytes;
    @NotNull
    private final MappedFile mappedFile;
    @NotNull
    private final Wire mappedWire;

    /**
     * used by {@link Demarshallable}
     *
     * @param wire a wire
     */
    @SuppressWarnings("unused")
    @UsedViaReflection
    private SingleTableStore(@NotNull final WireIn wire) {
        assert wire.startUse();
        try {
            this.wireType = Objects.requireNonNull(wire.read(MetaDataField.wireType).object(WireType.class));
            this.mappedBytes = (MappedBytes) (wire.bytes());
            this.mappedFile = mappedBytes.mappedFile();

            wire.consumePadding();
            if (wire.bytes().readRemaining() > 0) {
                this.metadata = Objects.requireNonNull(wire.read(MetaDataField.metadata).typedMarshallable());
            } else {
                //noinspection unchecked
                this.metadata = (T) Metadata.NoMeta.INSTANCE;
            }

            mappedWire = wireType.apply(mappedBytes);
            mappedWire.usePadding(true);

            disableThreadSafetyCheck(true);
        } finally {
            assert wire.endUse();
        }
    }

    /**
     * @param wireType    the wire type that is being used
     * @param mappedBytes used to mapped the data store file
     */
    SingleTableStore(@NotNull final WireType wireType,
                     @NotNull final MappedBytes mappedBytes,
                     @NotNull final T metadata) {
        this.wireType = wireType;
        this.metadata = metadata;
        this.mappedBytes = mappedBytes;
        this.mappedFile = mappedBytes.mappedFile();
        mappedWire = wireType.apply(mappedBytes);
        mappedWire.usePadding(true);

        disableThreadSafetyCheck(true);
    }

    public static <T, R> R doWithSharedLock(@NotNull final File file,
                                            @NotNull final Function<T, ? extends R> code,
                                            @NotNull final Supplier<T> target) {
        return doWithLock(file, code, target, true);
    }

    public static <T, R> R doWithExclusiveLock(@NotNull final File file,
                                               @NotNull final Function<T, ? extends R> code,
                                               @NotNull final Supplier<T> target) {
        return doWithLock(file, code, target, false);
    }

    // shared vs exclusive - see https://docs.oracle.com/javase/7/docs/api/java/nio/channels/FileChannel.html
    private static <T, R> R doWithLock(@NotNull final File file,
                                       @NotNull final Function<T, ? extends R> code,
                                       @NotNull final Supplier<T> target,
                                       final boolean shared) {
        final String type = shared ? "shared" : "exclusive";
        final StandardOpenOption readOrWrite = shared ? StandardOpenOption.READ : StandardOpenOption.WRITE;

        final long timeoutAt = System.currentTimeMillis() + timeoutMS;
        final long startMs = System.currentTimeMillis();
        try (final FileChannel channel = FileChannel.open(file.toPath(), readOrWrite)) {
            for (int count = 1; System.currentTimeMillis() < timeoutAt; count++) {
                try (FileLock fileLock = channel.tryLock(EXCLUSIVE_LOCK_START, EXCLUSIVE_LOCK_SIZE, shared)) {
                    if (fileLock != null) {
                        return code.apply(target.get());
                    }
                } catch (IOException | OverlappingFileLockException e) {
                    // failed to acquire the lock, wait until other operation completes
                    if (count > 9) {
                        if (Jvm.isDebugEnabled(SingleTableStore.class)) {
                            final long elapsedMs = System.currentTimeMillis() - startMs;
                            final String message = "Failed to acquire " + type + " lock on the table store file. Retrying, file=" + file.getAbsolutePath() + ", count=" + count + ", elapsed=" + elapsedMs + " ms";
                            Jvm.debug().on(SingleTableStore.class, "", new StackTrace(message));
                        }
                    }
                }
                int delay = Math.min(250, count * count);
                sleep(delay, MILLISECONDS);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Couldn't perform operation with " + type + " file lock", e);
        }
        throw new IllegalStateException("Unable to claim exclusive " + type + " lock on file " + file);
    }

    @NotNull
    @Override
    public File file() {
        return mappedFile.file();
    }

    @NotNull
    @Override
    public String dump() {
        return dump(false);
    }

    private String dump(final boolean abbrev) {

        final MappedBytes bytes = MappedBytes.mappedBytes(mappedFile);
        try {
            bytes.readLimit(bytes.realCapacity());
            return Wires.fromSizePrefixedBlobs(bytes, true, abbrev);
        } finally {
            bytes.releaseLast();
        }
    }

    @NotNull
    @Override
    public String shortDump() {
        throwExceptionIfClosed();

        return dump(true);
    }

    @Override
    protected void performClose() {
        mappedBytes.releaseLast();
    }

    /**
     * @return creates a new instance of mapped bytes, because, for example the tailer and appender can be at different locations.
     */
    @NotNull
    @Override
    public MappedBytes bytes() {
        throwExceptionIfClosed();

        return MappedBytes.mappedBytes(mappedFile);
    }

    @NotNull
    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "wireType=" + wireType +
                ", mappedFile=" + mappedFile +
                '}';
    }

    // *************************************************************************
    // Marshalling
    // *************************************************************************

    private void onCleanup() {
        mappedBytes.releaseLast();
    }

    @Override
    public void writeMarshallable(@NotNull final WireOut wire) {

        wire.write(MetaDataField.wireType).object(wireType);

        if (metadata != Metadata.NoMeta.INSTANCE)
            wire.write(MetaDataField.metadata).typedMarshallable(this.metadata);

        // align to a word whether needed or not as a micro-optimisation.
        wire.writeAlignTo(Integer.BYTES, 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized LongValue acquireValueFor(CharSequence key, final long defaultValue) { // TODO Change to ThreadLocal values if performance is a problem.

        if (mappedBytes.isClosed())
            throw new ClosedIllegalStateException("Closed");

        final StringBuilder sb = Wires.acquireStringBuilder();
        mappedBytes.reserve(this);
        try {
            mappedBytes.readPosition(0);
            mappedBytes.readLimit(mappedBytes.realCapacity());
            while (mappedWire.readDataHeader()) {
                final int header = mappedBytes.readVolatileInt();
                if (Wires.isNotComplete(header))
                    break;
                final long readPosition = mappedBytes.readPosition();
                final int length = Wires.lengthOf(header);
                final ValueIn valueIn = mappedWire.readEventName(sb);
                if (StringUtils.equalsCaseIgnore(key, sb)) {
                    return valueIn.int64ForBinding(null);
                }
                mappedBytes.readPosition(readPosition + length);
            }
            mappedBytes.writeLimit(mappedBytes.realCapacity());
            long start = mappedBytes.readPosition();
            mappedBytes.writePosition(start);
            final long pos = mappedWire.enterHeader(128L);
            final LongValue longValue = wireType.newLongReference().get();
            mappedWire.writeEventName(key).int64forBinding(defaultValue, longValue);
            mappedWire.writeAlignTo(Integer.BYTES, 0);
            mappedWire.updateHeader(pos, false, 0);
            long end = mappedBytes.writePosition();
            long chuckSize = mappedFile.chunkSize();
            long overlapSize = mappedFile.overlapSize();
            long endOfChunk = (start + chuckSize - 1) / chuckSize * chuckSize;
            if (end >= endOfChunk + overlapSize)
                throw new IllegalStateException("Misaligned write");
            return longValue;

        } catch (StreamCorruptedException | EOFException e) {
            throw new IORuntimeException(e);

        } finally {
            mappedBytes.release(this);
        }
    }

    @Override
    public synchronized <T> void forEachKey(T accumulator, TableStoreIterator<T> tsIterator) {
        final StringBuilder sb = Wires.acquireStringBuilder();
        mappedBytes.reserve(this);
        try {
            mappedBytes.readPosition(0);
            mappedBytes.readLimit(mappedBytes.realCapacity());
            while (mappedWire.readDataHeader()) {
                final int header = mappedBytes.readVolatileInt();
                if (Wires.isNotComplete(header))
                    break;
                final long readPosition = mappedBytes.readPosition();
                final int length = Wires.lengthOf(header);
                final ValueIn valueIn = mappedWire.readEventName(sb);
                tsIterator.accept(accumulator, sb, valueIn);
                mappedBytes.readPosition(readPosition + length);
            }

        } catch (EOFException e) {
            throw new IORuntimeException(e);

        } finally {
            mappedBytes.release(this);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R> R doWithExclusiveLock(@NotNull final Function<TableStore<T>, ? extends R> code) {
        return doWithExclusiveLock(file(), code, () -> this);
    }

    @Override
    public T metadata() {
        return metadata;
    }

}