/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.impl.AbstractChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

import java.io.File;

import static net.openhft.chronicle.core.pool.ClassAliasPool.CLASS_ALIASES;

public class SingleChronicleQueueBuilder extends AbstractChronicleQueueBuilder<SingleChronicleQueueBuilder, SingleChronicleQueue> {
    static {
        CLASS_ALIASES.addAlias(WireType.class);
        CLASS_ALIASES.addAlias(SingleChronicleQueueStore.Roll.class, "SCQSRoll");
        CLASS_ALIASES.addAlias(SingleChronicleQueueStore.Indexing.class, "SCQSIndexing");
        CLASS_ALIASES.addAlias(SingleChronicleQueueStore.class, "SCQStore");
    }

    @SuppressWarnings("unchecked")
    public SingleChronicleQueueBuilder(@NotNull String path) {
        this(new File(path));
    }

    @SuppressWarnings("unchecked")
    public SingleChronicleQueueBuilder(@NotNull File path) {
        super(path);
    }

    public static void init() {
        // Call static block.
    }

    @NotNull
    public static SingleChronicleQueueBuilder binary(@NotNull File name) {
        return new SingleChronicleQueueBuilder(name)
                .wireType(WireType.BINARY);
    }

    @NotNull
    public static SingleChronicleQueueBuilder text(@NotNull File name) {
        return new SingleChronicleQueueBuilder(name)
                .wireType(WireType.TEXT);
    }

    // *************************************************************************
    //
    // *************************************************************************

    @NotNull
    public SingleChronicleQueue build() {
        if (buffered())
            log.warn("Buffering is only supported in Chronicle Queue Enterprise");
        return new SingleChronicleQueue(clone());
    }

    // *************************************************************************
    //
    // *************************************************************************

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
