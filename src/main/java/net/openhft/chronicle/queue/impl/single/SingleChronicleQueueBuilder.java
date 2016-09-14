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
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.queue.impl.AbstractChronicleQueueBuilder;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.nio.file.Path;

import static net.openhft.chronicle.core.pool.ClassAliasPool.CLASS_ALIASES;
import static net.openhft.chronicle.wire.WireType.DEFAULT_ZERO_BINARY;

public class SingleChronicleQueueBuilder extends AbstractChronicleQueueBuilder<SingleChronicleQueueBuilder, SingleChronicleQueue> {
    static {
        CLASS_ALIASES.addAlias(WireType.class);
        CLASS_ALIASES.addAlias(SCQRoll.class, "SCQSRoll");
        CLASS_ALIASES.addAlias(SCQIndexing.class, "SCQSIndexing");
        CLASS_ALIASES.addAlias(SingleChronicleQueueStore.class, "SCQStore");
        CLASS_ALIASES.addAlias(TimedStoreRecovery.class);
    }

    @SuppressWarnings("unchecked")
    @Deprecated
    public SingleChronicleQueueBuilder(@NotNull String path) {
        this(new File(path));
    }

    @SuppressWarnings("unchecked")
    @Deprecated
    public SingleChronicleQueueBuilder(@NotNull File path) {
        super(path);
        storeFactory(SingleChronicleQueueBuilder::createStore);
    }

    public static void init() {
        // make sure the static block has been called.
    }

    @NotNull
    public static SingleChronicleQueueBuilder binary(@NotNull Path path) {
        return binary(path.toFile());
    }

    @NotNull
    public static SingleChronicleQueueBuilder binary(@NotNull String basePath) {
        return binary(new File(basePath));
    }

    @NotNull
    public static SingleChronicleQueueBuilder defaultZeroBinary(@NotNull String basePath) {
        return defaultZeroBinary(new File(basePath));
    }

    @NotNull
    public static SingleChronicleQueueBuilder binary(@NotNull File basePathFile) {
        return new SingleChronicleQueueBuilder(basePathFile)
                .wireType(WireType.BINARY_LIGHT);
    }

    @NotNull
    public static SingleChronicleQueueBuilder defaultZeroBinary(@NotNull File basePathFile) {
        return new SingleChronicleQueueBuilder(basePathFile)
                .wireType(DEFAULT_ZERO_BINARY);
    }

    @Deprecated
    @NotNull
    public static SingleChronicleQueueBuilder text(@NotNull File name) {
        return new SingleChronicleQueueBuilder(name)
                .wireType(WireType.TEXT);
    }

    // *************************************************************************
    //
    // *************************************************************************

    @NotNull
    static SingleChronicleQueueStore createStore(RollingChronicleQueue queue, Wire wire) {
        final SingleChronicleQueueStore wireStore = new SingleChronicleQueueStore(
                queue.rollCycle(),
                queue.wireType(),
                (MappedBytes) wire.bytes(),
                queue.epoch(),
                queue.indexCount(),
                queue.indexSpacing(),
                queue.recoverySupplier().apply(queue.wireType()),
                queue.deltaCheckpointInterval());

        wire.writeEventName(MetaDataKeys.header).typedMarshallable(wireStore);

        return wireStore;
    }

    // *************************************************************************
    //
    // *************************************************************************

    @NotNull
    public SingleChronicleQueue build() {
        if (buffered())
            getLogger().warn("Buffering is only supported in Chronicle Queue Enterprise");
        return new SingleChronicleQueue(this);
    }

    @NotNull
    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    @Override
    public SingleChronicleQueueBuilder clone() {
        try {
            return (SingleChronicleQueueBuilder) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }
}
