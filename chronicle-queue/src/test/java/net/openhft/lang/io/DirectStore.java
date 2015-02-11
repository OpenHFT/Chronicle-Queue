/*
 * Copyright 2014 Higher Frequency Trading
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

package net.openhft.lang.io;

import net.openhft.lang.io.serialization.BytesMarshallableSerializer;
import net.openhft.lang.io.serialization.BytesMarshallerFactory;
import net.openhft.lang.io.serialization.JDKZObjectSerializer;
import net.openhft.lang.io.serialization.ObjectSerializer;
import net.openhft.lang.io.serialization.impl.VanillaBytesMarshallerFactory;
import net.openhft.lang.model.constraints.NotNull;
import sun.misc.Cleaner;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author peter.lawrey
 */
public class DirectStore implements BytesStore, AutoCloseable {
    private final ObjectSerializer objectSerializer;
    private final Cleaner cleaner;
    private final Deallocator deallocator;
    private long address;
    private long size;
    private final AtomicInteger refCount = new AtomicInteger(1);

    public DirectStore(long size) {
        this(new VanillaBytesMarshallerFactory(), size);
    }

    private DirectStore(BytesMarshallerFactory bytesMarshallerFactory, long size) {
        this(bytesMarshallerFactory, size, true);
    }

    private DirectStore(BytesMarshallerFactory bytesMarshallerFactory, long size, boolean zeroOut) {
        this(BytesMarshallableSerializer.create(bytesMarshallerFactory, JDKZObjectSerializer.INSTANCE), size, zeroOut);
    }

    public DirectStore(ObjectSerializer objectSerializer, long size, boolean zeroOut) {
        this.objectSerializer = objectSerializer;
        address = NativeBytes.UNSAFE.allocateMemory(size);

//        System.out.println("old value " + Integer.toHexString(NativeBytes.UNSAFE.getInt(null, address)));
        if (zeroOut) {
            NativeBytes.UNSAFE.setMemory(address, size, (byte) 0);
            NativeBytes.UNSAFE.putLongVolatile(null, address, 0L);
        }

        this.size = size;
        deallocator = new Deallocator(address);
        cleaner = Cleaner.create(this, deallocator);
    }

    @Override
    public ObjectSerializer objectSerializer() {
        return objectSerializer;
    }

    @NotNull
    public static DirectStore allocate(long size) {
        return new DirectStore(null, size);
    }

    @NotNull
    public static DirectStore allocateLazy(long size) {
        return new DirectStore((BytesMarshallerFactory) null, size, false);
    }

    /**
     * Resizes this {@code DirectStore} to the {@code newSize}.
     *
     * <p>If {@code zeroOut} is {@code false}, the memory past the old size is not zeroed out and
     * will generally be garbage.
     *
     * <p>{@code DirectStore} don't keep track of the child {@code DirectBytes} instances, so after
     * the resize they might point to the wrong memory. Use at your own risk.
     *
     * @param newSize new size of this {@code DirectStore}
     * @param zeroOut if the memory past the old size should be zeroed out on increasing resize
     * @throws IllegalArgumentException if the {@code newSize} is not positive
     */
    public void resize(long newSize, boolean zeroOut) {
        if (newSize <= 0)
            throw new IllegalArgumentException("Given newSize is " + newSize +
                    " but should be positive");
        address = deallocator.address = NativeBytes.UNSAFE.reallocateMemory(address, newSize);
        if (zeroOut && newSize > size) {
            NativeBytes.UNSAFE.setMemory(address + size, newSize - size, (byte) 0);
        }
        size = newSize;
    }

    @SuppressWarnings("ConstantConditions")
    @NotNull
    public DirectBytes bytes() {
        boolean debug = false;
        assert debug = true;
        return debug ? new BoundsCheckingDirectBytes(this, refCount) : new DirectBytes(this, refCount);
    }

    @NotNull
    public DirectBytes bytes(long offset, long length) {
        return new DirectBytes(this, refCount, offset, length);
    }

    @Override
    public long address() {
        return address;
    }

    public void free() {
        cleaner.clean();
    }

    public long size() {
        return size;
    }

    public static BytesStore allocateLazy(long sizeInBytes, ObjectSerializer objectSerializer) {
        return new DirectStore(objectSerializer, sizeInBytes, false);
    }

    @Override
    public File file() {
        return null;
    }

    /**
     * calls free
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        free();
    }

    /**
     * Static nested class instead of anonymous because the latter would hold a strong reference to
     * this DirectStore preventing it from becoming phantom-reachable.
     */
    private static class Deallocator implements Runnable {
        private volatile long address;

        Deallocator(long address) {
            assert address != 0;
            this.address = address;
        }

        @Override
        public void run() {
            if (address == 0)
                return;
            NativeBytes.UNSAFE.freeMemory(address);
            address = 0;
        }
    }
}
