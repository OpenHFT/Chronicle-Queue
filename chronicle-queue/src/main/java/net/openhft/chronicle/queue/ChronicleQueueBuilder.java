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
package net.openhft.chronicle.queue;

import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;

/**
 * Created by peter.lawrey on 30/01/15.
 */
public class ChronicleQueueBuilder implements Cloneable {
    private String name;
    private long blockSize;
    private Class<? extends Wire> wireType;

    public ChronicleQueueBuilder(File name) {
        this(name.getAbsolutePath());
    }

    public ChronicleQueueBuilder(String name) {
        this.name = name;
        this.blockSize = 64L << 20;
        this.wireType = BinaryWire.class;
    }

    public String name() {
        return this.name;
    }

    public ChronicleQueueBuilder blockSize(int blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    public long blockSize() {
        return this.blockSize;
    }

    public ChronicleQueueBuilder wireType(Class<? extends Wire> wireType) {
        this.wireType = wireType;
        return this;
    }

    public Class<? extends Wire> wireType() {
        return this.wireType;
    }

    @NotNull
    public ChronicleQueue build() throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @NotNull
    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    @Override
    public ChronicleQueueBuilder clone() {
        try {
            return (ChronicleQueueBuilder) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    public static ChronicleQueueBuilder binary(File name) {
        return binary(name.getAbsolutePath());
    }

    public static ChronicleQueueBuilder binary(String name) {
        return new ChronicleQueueBuilder(name)
                .wireType(BinaryWire.class);
    }

    public static ChronicleQueueBuilder text(File name) {
        return text(name.getAbsolutePath());
    }

    public static ChronicleQueueBuilder text(String name) {
        return new ChronicleQueueBuilder(name)
                .wireType(TextWire.class);
    }
}
