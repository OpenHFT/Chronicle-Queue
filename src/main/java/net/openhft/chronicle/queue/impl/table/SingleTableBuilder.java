/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.util.StringUtils;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.single.MetaDataKeys;
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

import static net.openhft.chronicle.core.pool.ClassAliasPool.CLASS_ALIASES;

public class SingleTableBuilder<T extends Metadata> {

    static {
        CLASS_ALIASES.addAlias(WireType.class);
        CLASS_ALIASES.addAlias(SingleTableStore.class, "STStore");
    }

    @NotNull
    private final File file;
    @NotNull
    private T metadata;

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
        if (readOnly && !file.exists())
            throw new IORuntimeException("File not found in readOnly mode");

        try {
            if (!readOnly && file.createNewFile() && !file.canWrite())
                throw new IllegalStateException("Cannot write to tablestore file " + file);
            MappedBytes bytes = MappedBytes.mappedBytes(file, 64 << 10, 0, readOnly);
            // eagerly initialize backing MappedFile page - otherwise wire.writeFirstHeader() will try to lock the file
            // to allocate the first byte store and that will cause lock overlap
            bytes.readVolatileInt(0);
            Wire wire = wireType.apply(bytes);
            if (readOnly)
                return SingleTableStore.doWithSharedLock(file, (v) -> {
                    try {
                        return readTableStore(wire);
                    } catch (IOException ex) {
                        throw Jvm.rethrow(ex);
                    }
                }, () -> null);
            else
                return SingleTableStore.doWithExclusiveLock(file, (v) -> {
                    try {
                        if (wire.writeFirstHeader()) {
                            return writeTableStore(bytes, wire);
                        } else {
                            return readTableStore(wire);
                        }
                    } catch (IOException ex) {
                        throw Jvm.rethrow(ex);
                    }
                }, () -> null);
        } catch (IOException e) {
            throw new IORuntimeException("file=" + file.getAbsolutePath(), e);
        }
    }

    @NotNull
    private TableStore<T> readTableStore(Wire wire) throws StreamCorruptedException {
        wire.readFirstHeader();

        StringBuilder name = Wires.acquireStringBuilder();
        ValueIn valueIn = wire.readEventName(name);
        if (StringUtils.isEqual(name, MetaDataKeys.header.name())) {
            @NotNull TableStore<T> existing = Objects.requireNonNull(valueIn.typedMarshallable());
            metadata.overrideFrom(existing.metadata());
            return existing;
        } else {
            //noinspection unchecked
            throw new StreamCorruptedException("The first message should be the header, was " + name);
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
    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    @Override
    public SingleTableBuilder<T> clone() {
        try {
            @SuppressWarnings("unchecked")
            SingleTableBuilder<T> clone = (SingleTableBuilder) super.clone();
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
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