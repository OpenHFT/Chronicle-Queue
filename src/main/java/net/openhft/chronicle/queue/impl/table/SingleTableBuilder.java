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
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.util.StringUtils;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.single.MetaDataKeys;
import net.openhft.chronicle.queue.impl.single.StoreRecovery;
import net.openhft.chronicle.queue.impl.single.StoreRecoveryFactory;
import net.openhft.chronicle.queue.impl.single.TimedStoreRecovery;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static net.openhft.chronicle.core.pool.ClassAliasPool.CLASS_ALIASES;

public class SingleTableBuilder {
    public static final String SUFFIX = ".cq4t";

    static {
        CLASS_ALIASES.addAlias(WireType.class);
        CLASS_ALIASES.addAlias(SingleTableStore.class, "STStore");
        CLASS_ALIASES.addAlias(TimedStoreRecovery.class);
    }

    @NotNull
    private final File file;

    private WireType wireType;
    private boolean readOnly;
    private StoreRecoveryFactory recoverySupplier = TimedStoreRecovery.FACTORY;
    private long timeoutMS = TimeUnit.SECONDS.toMillis(5);

    private SingleTableBuilder(@NotNull File path) {
        this.file = path;
    }

    @NotNull
    public static SingleTableBuilder builder(@NotNull Path path, @NotNull WireType wireType) {
        return builder(path.toFile(), wireType);
    }

    @NotNull
    public static SingleTableBuilder builder(@NotNull File file, @NotNull WireType wireType) {
        if (file.isDirectory()) {
            throw new IllegalArgumentException("Tables should be configured with the table file, not a directory. Actual file used: " + file.getParentFile());
        }
        if (!file.getName().endsWith(SUFFIX)) {
            throw new IllegalArgumentException("Invalid file type: " + file.getName());
        }

        return new SingleTableBuilder(file).wireType(wireType);
    }

    @NotNull
    public static SingleTableBuilder binary(@NotNull Path path) {
        return binary(path.toFile());
    }

    @NotNull
    public static SingleTableBuilder binary(@NotNull String file) {
        return binary(new File(file));
    }

    @NotNull
    public static SingleTableBuilder binary(@NotNull File basePathFile) {
        return builder(basePathFile, WireType.BINARY_LIGHT);
    }

    // *************************************************************************
    //
    // *************************************************************************

    @NotNull
    public TableStore build() {
        if (readOnly && !file.exists()) {
            throw new IORuntimeException("File not found in readOnly mode");
        }
        try {
            MappedBytes bytes = MappedBytes.mappedBytes(file, 64 << 10, 0, readOnly);
            Wire wire = wireType.apply(bytes);
            StoreRecovery recovery = recoverySupplier.apply(wireType);
            try {
                TableStore tableStore;
                if ((!readOnly) && wire.writeFirstHeader()) {
                    tableStore = writeTableStore(bytes, wire, recovery);

                } else {
                    wire.readFirstHeader(timeoutMS, TimeUnit.MILLISECONDS);

                    StringBuilder name = Wires.acquireStringBuilder();
                    ValueIn valueIn = wire.readEventName(name);
                    if (StringUtils.isEqual(name, MetaDataKeys.header.name())) {
                        tableStore = valueIn.typedMarshallable();
                    } else {
                        //noinspection unchecked
                        throw new StreamCorruptedException("The first message should be the header, was " + name);
                    }
                }
                return tableStore;

            } catch (TimeoutException e) {
                recovery.recoverAndWriteHeader(wire, 10_000, null, null);
                return writeTableStore(bytes, wire, recovery);
            }
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }
    }

    @NotNull
    private TableStore writeTableStore(MappedBytes bytes, Wire wire, StoreRecovery recovery) throws EOFException, StreamCorruptedException {
        TableStore store = new SingleTableStore(wireType, bytes, recovery);
        wire.writeEventName("header").object(store);
        wire.updateFirstHeader();
        return store;
    }

    @NotNull
    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    @Override
    public SingleTableBuilder clone() {
        try {
            @SuppressWarnings("unchecked")
            SingleTableBuilder clone = (SingleTableBuilder) super.clone();
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

    public SingleTableBuilder wireType(WireType wireType) {
        this.wireType = wireType;
        return this;
    }

    public boolean readOnly() {
        return readOnly;
    }

    public SingleTableBuilder readOnly(boolean readOnly) {
        this.readOnly = readOnly;
        return this;
    }

    public StoreRecoveryFactory recoverySupplier() {
        return recoverySupplier;
    }

    public SingleTableBuilder recoverySupplier(StoreRecoveryFactory recoverySupplier) {
        this.recoverySupplier = recoverySupplier;
        return this;
    }

    public long timeoutMS() {
        return timeoutMS;
    }

    public SingleTableBuilder timeoutMS(long timeoutMS) {
        this.timeoutMS = timeoutMS;
        return this;
    }
}