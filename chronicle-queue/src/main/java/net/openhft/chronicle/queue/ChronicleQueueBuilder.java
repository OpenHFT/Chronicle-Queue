/*
 * Copyright 2015 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.SingleChronicleQueue;
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
    public SingleChronicleQueue build() throws IOException {
        return new SingleChronicleQueue(this);
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
