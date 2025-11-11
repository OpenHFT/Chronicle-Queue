/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.scoped.ScopedResource;
import net.openhft.chronicle.core.util.Builder;
import net.openhft.chronicle.core.util.StringUtils;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.single.MetaDataKeys;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.threads.TimingPauser;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static net.openhft.chronicle.core.pool.ClassAliasPool.CLASS_ALIASES;

public class SingleTableBuilder<T extends Metadata> implements Builder<TableStore<T>> {

    static {
        CLASS_ALIASES.addAlias(WireType.class);
        CLASS_ALIASES.addAlias(SingleTableStore.class, "STStore");
    }

    @NotNull
    private final File file;
    @NotNull
    private final T metadata;

    private WireType wireType;
    private boolean readOnly;

    private SingleTableBuilder(@NotNull File path, @NotNull T metadata) {
        this.file = path;
        this.metadata = metadata;
    }

    @NotNull
    public static <T extends Metadata> SingleTableBuilder<T> builder(@NotNull File file, @NotNull WireType wireType, @NotNull T metadata) {
        if (file.isDirectory()) {
            throw new IllegalArgumentException("Tables should be configured with the table file, not a directory. Actual file used: " + file.getParentFile());
        }
        if (!file.getName().endsWith(SingleTableStore.SUFFIX)) {
            throw new IllegalArgumentException("Invalid file type: " + file.getName());
        }

        return new SingleTableBuilder<>(file, metadata).wireType(wireType);
    }

    @NotNull
    public static <T extends Metadata> SingleTableBuilder<T> binary(@NotNull Path path, @NotNull T metadata) {
        return binary(path.toFile(), metadata);
    }

    @NotNull
    public static <T extends Metadata> SingleTableBuilder<T> binary(@NotNull String file, @NotNull T metadata) {
        return binary(new File(file), metadata);
    }

    @NotNull
    public static <T extends Metadata> SingleTableBuilder<T> binary(@NotNull File basePathFile, @NotNull T metadata) {
        return builder(basePathFile, WireType.BINARY_LIGHT, metadata);
    }

    // *************************************************************************
    //
    // *************************************************************************

    @NotNull
    public TableStore<T> build() {
        if (readOnly) {
            if (!file.exists())
                throw new IORuntimeException("Metadata file not found in readOnly mode");

            // Wait a short time for the file to be initialized
            TimingPauser pauser = Pauser.balanced();
            try {
                while (file.length() < OS.mapAlignment()) {
                    pauser.pause(1, TimeUnit.SECONDS);
                }
            } catch (TimeoutException e) {
                throw new IORuntimeException("Metadata file found in readOnly mode, but not initialized yet");
            }
        }

        MappedBytes bytes = null;
        try {
            if (!readOnly && file.createNewFile() && !file.canWrite()) {
                throw new IllegalStateException("Cannot write to tablestore file " + file);
            }
            // TODO Change this to a single chunk file in x.28
            bytes = MappedBytes.mappedBytes(file, OS.SAFE_PAGE_SIZE, OS.SAFE_PAGE_SIZE, readOnly);
            // these MappedBytes are shared, but the assumption is they shouldn't grow. Supports 2K entries.
            bytes.singleThreadedCheckDisabled(true);

            // eagerly initialize backing MappedFile page - otherwise wire.writeFirstHeader() will try to lock the file
            // to allocate the first byte store and that will cause lock overlap
            bytes.readVolatileInt(0);
            Wire wire = wireType.apply(bytes);
            if (readOnly) {
                return SingleTableStore.doWithSharedLock(file, v -> {
                    try {
                        return readTableStore(wire);
                    } catch (IOException ex) {
                        throw Jvm.rethrow(ex);
                    }
                }, () -> null);
            } else {
                MappedBytes finalBytes = bytes;
                return SingleTableStore.doWithExclusiveLock(file, v -> {
                    try {
                        if (wire.writeFirstHeader()) {
                            return writeTableStore(finalBytes, wire);
                        } else {
                            return readTableStore(wire);
                        }
                    } catch (IOException ex) {
                        throw Jvm.rethrow(ex);
                    }
                }, () -> null);
            }
        } catch (IOException e) {
            throw new IORuntimeException("file=" + file.getAbsolutePath(), e);
        } finally {
            if (bytes != null)
                bytes.singleThreadedCheckReset();
        }
    }

    @NotNull
    private TableStore<T> readTableStore(Wire wire) throws StreamCorruptedException {
        wire.readFirstHeader();

        final ValueIn valueIn = readTableStoreValue(wire);
        @NotNull TableStore<T> existing = Objects.requireNonNull(valueIn.typedMarshallable());
        metadata.overrideFrom(existing.metadata());
        return existing;
    }

    private ValueIn readTableStoreValue(@NotNull Wire wire) throws StreamCorruptedException {
        try (ScopedResource<StringBuilder> stlSb = Wires.acquireStringBuilderScoped()) {
            StringBuilder name = stlSb.get();
            ValueIn valueIn = wire.readEventName(name);
            if (!StringUtils.isEqual(name, MetaDataKeys.header.name())) {
                throw new StreamCorruptedException("The first message should be the header, was " + name);
            }
            return valueIn;
        }
    }

    @NotNull
    private TableStore<T> writeTableStore(MappedBytes bytes, Wire wire) {
        TableStore<T> store = new SingleTableStore<>(wireType, bytes, metadata);
        wire.writeEventName("header").object(store);
        wire.updateFirstHeader();
        return store;
    }

    @NotNull
    public File file() {
        return file;
    }

    public WireType wireType() {
        return wireType;
    }

    public SingleTableBuilder<T> wireType(WireType wireType) {
        this.wireType = wireType;
        return this;
    }

    public boolean readOnly() {
        return readOnly;
    }

    public SingleTableBuilder<T> readOnly(boolean readOnly) {
        this.readOnly = readOnly;
        return this;
    }
}
