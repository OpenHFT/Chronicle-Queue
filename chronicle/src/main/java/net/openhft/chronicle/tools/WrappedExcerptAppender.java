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
package net.openhft.chronicle.tools;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.lang.io.*;
import net.openhft.lang.io.serialization.ObjectSerializer;
import net.openhft.lang.model.constraints.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.Map;

public class WrappedExcerptAppender<T extends ExcerptAppender> implements ExcerptAppender {
    protected T wrappedAppender;

    public WrappedExcerptAppender(final @NotNull T appender) {
        this.wrappedAppender = appender;
    }

    public void startExcerpt() {
        wrappedAppender.startExcerpt();
    }

    public ByteBuffer sliceAsByteBuffer(@Nullable ByteBuffer toReuse) {
        return wrappedAppender.sliceAsByteBuffer(toReuse);
    }

    public void readMarshallable(@NotNull Bytes in) throws IllegalStateException {
        wrappedAppender.readMarshallable(in);
    }

    public long readCompactLong() {
        return wrappedAppender.readCompactLong();
    }

    public boolean tryLockNanosInt(long offset, long nanos) {
        return wrappedAppender.tryLockNanosInt(offset, nanos);
    }

    public void writeMarshallable(@NotNull Bytes out) {
        wrappedAppender.writeMarshallable(out);
    }

    public int readInt24() {
        return wrappedAppender.readInt24();
    }

    public void flush() {
        wrappedAppender.flush();
    }

    public void writeDouble(long offset, double v) {
        wrappedAppender.writeDouble(offset, v);
    }

    public long limit() {
        return wrappedAppender.limit();
    }

    @NotNull
    public ByteStringAppender appendTimeMillis(long timeInMS) {
        return wrappedAppender.appendTimeMillis(timeInMS);
    }

    @Nullable
    public <E extends Enum<E>> E parseEnum(@NotNull Class<E> eClass, @NotNull StopCharTester tester) throws BufferUnderflowException {
        return wrappedAppender.parseEnum(eClass, tester);
    }

    public int refCount() {
        return wrappedAppender.refCount();
    }

    public void writeShort(long offset, int v) {
        wrappedAppender.writeShort(offset, v);
    }

    public <E> void writeEnum(@Nullable E e) {
        wrappedAppender.writeEnum(e);
    }

    @NotNull
    public <E> ByteStringAppender append(@NotNull Iterable<E> list, @NotNull CharSequence separator) {
        return wrappedAppender.append(list, separator);
    }

    public void writeCompactUnsignedShort(int v) {
        wrappedAppender.writeCompactUnsignedShort(v);
    }

    public long readVolatileLong() {
        return wrappedAppender.readVolatileLong();
    }

    public void write(RandomDataInput bytes, long position, long length) {
        wrappedAppender.write(bytes, position, length);
    }

    public void writeOrderedInt(int v) {
        wrappedAppender.writeOrderedInt(v);
    }

    public boolean readUTFΔ(@NotNull StringBuilder stringBuilder) {
        return wrappedAppender.readUTFΔ(stringBuilder);
    }

    public void writeInt48(long offset, long v) {
        wrappedAppender.writeInt48(offset, v);
    }

    public long readLong() {
        return wrappedAppender.readLong();
    }

    public void writeLong(long v) {
        wrappedAppender.writeLong(v);
    }

    @NotNull
    public ByteStringAppender appendDateTimeMillis(long timeInMS) {
        return wrappedAppender.appendDateTimeMillis(timeInMS);
    }

    @Nullable
    public <E> E readEnum(@NotNull Class<E> eClass) {
        return wrappedAppender.readEnum(eClass);
    }

    public void write(RandomDataInput bytes) {
        wrappedAppender.write(bytes);
    }

    @NotNull
    public ByteStringAppender append(double d) {
        return wrappedAppender.append(d);
    }

    @NotNull
    public String toDebugString() {
        return wrappedAppender.toDebugString();
    }

    public boolean isFinished() {
        return wrappedAppender.isFinished();
    }

    public void writeCompactUnsignedInt(long v) {
        wrappedAppender.writeCompactUnsignedInt(v);
    }

    @NotNull
    public ByteStringAppender append(double d, int precision) {
        return wrappedAppender.append(d, precision);
    }

    public int readUnsignedByteOrThrow() throws BufferUnderflowException {
        return wrappedAppender.readUnsignedByteOrThrow();
    }

    public Bytes zeroOut(long start, long end) {
        wrappedAppender.zeroOut(start, end);
        return this;
    }

    @Override
    public Bytes zeroOut(long start, long end, boolean ifNotZero) {
        wrappedAppender.zeroOut(start, end, ifNotZero);
        return this;
    }

    public void writeShort(int v) {
        wrappedAppender.writeShort(v);
    }

    public short addShort(long offset, short s) {
        return wrappedAppender.addShort(offset, s);
    }

    public void writeUnsignedInt(long v) {
        wrappedAppender.writeUnsignedInt(v);
    }

    public void free() {
        wrappedAppender.free();
    }

    public int readUnsignedShort() {
        return wrappedAppender.readUnsignedShort();
    }

    public void writeStopBit(long n) {
        wrappedAppender.writeStopBit(n);
    }

    @Nullable
    public <T> T readObject(Class<T> tClass) throws IllegalStateException {
        return wrappedAppender.readObject(tClass);
    }

    public void writeCompactInt(int v) {
        wrappedAppender.writeCompactInt(v);
    }

    public void writeOrderedLong(long v) {
        wrappedAppender.writeOrderedLong(v);
    }

    public byte addByte(long offset, byte b) {
        return wrappedAppender.addByte(offset, b);
    }

    public int readVolatileInt() {
        return wrappedAppender.readVolatileInt();
    }

    public void close() {
        wrappedAppender.close();
    }

    public void read(@NotNull ByteBuffer bb) {
        wrappedAppender.read(bb);
    }

    @NotNull
    public ByteStringAppender append(long l, int base) {
        return wrappedAppender.append(l, base);
    }

    public long skip(long n) {
        return wrappedAppender.skip(n);
    }

    public boolean selfTerminating() {
        return wrappedAppender.selfTerminating();
    }

    public void writeBytes(@NotNull String s) {
        wrappedAppender.writeBytes(s);
    }

    public long size() {
        return wrappedAppender.size();
    }

    public int readCompactUnsignedShort() {
        return wrappedAppender.readCompactUnsignedShort();
    }

    @NotNull
    public ByteStringAppender append(@NotNull CharSequence s, int start, int end) {
        return wrappedAppender.append(s, start, end);
    }

    public void writeCompactLong(long v) {
        wrappedAppender.writeCompactLong(v);
    }

    public double readCompactDouble() {
        return wrappedAppender.readCompactDouble();
    }

    public void writeOrderedInt(long offset, int v) {
        wrappedAppender.writeOrderedInt(offset, v);
    }

    public void writeObject(Object object, int start, int end) {
        wrappedAppender.writeObject(object, start, end);
    }

    public CharSequence asString() {
        return wrappedAppender.asString();
    }

    @Nullable
    public String readUTFΔ() {
        return wrappedAppender.readUTFΔ();
    }

    public Bytes flip() {
        return wrappedAppender.flip();
    }

    public int addInt(long offset, int i) {
        return wrappedAppender.addInt(offset, i);
    }

    public long readUnsignedInt(long offset) {
        return wrappedAppender.readUnsignedInt(offset);
    }

    public void writeByte(int v) {
        wrappedAppender.writeByte(v);
    }

    public void writeUnsignedInt(long offset, long v) {
        wrappedAppender.writeUnsignedInt(offset, v);
    }

    public void addPaddedEntry() {
        wrappedAppender.addPaddedEntry();
    }

    public void writeInt(int v) {
        wrappedAppender.writeInt(v);
    }

    public short readShort() {
        return wrappedAppender.readShort();
    }

    public Chronicle chronicle() {
        return wrappedAppender.chronicle();
    }

    public void writeUnsignedByte(long offset, int v) {
        wrappedAppender.writeUnsignedByte(offset, v);
    }

    public void asString(Appendable appendable) {
        wrappedAppender.asString(appendable);
    }

    public long readInt48(long offset) {
        return wrappedAppender.readInt48(offset);
    }

    public void unlockRWReadLock(long offset) throws IllegalStateException {
        wrappedAppender.unlockRWReadLock(offset);
    }

    @NotNull
    public String readUTF() {
        return wrappedAppender.readUTF();
    }

    public void writeUnsignedShort(long offset, int v) {
        wrappedAppender.writeUnsignedShort(offset, v);
    }

    public void readFully(@NotNull char[] data) {
        wrappedAppender.readFully(data);
    }

    public void writeInt24(long offset, int v) {
        wrappedAppender.writeInt24(offset, v);
    }

    public void writeChars(@NotNull CharSequence cs) {
        wrappedAppender.writeChars(cs);
    }

    public float readFloat(long offset) {
        return wrappedAppender.readFloat(offset);
    }

    public long capacity() {
        return wrappedAppender.capacity();
    }

    public CharSequence subSequence(int start, int end) {
        return wrappedAppender.subSequence(start, end);
    }

    public Bytes clear() {
        return wrappedAppender.clear();
    }

    @Nullable
    public String readUTFΔ(long offset) throws IllegalStateException {
        return wrappedAppender.readUTFΔ(offset);
    }

    @NotNull
    public ObjectSerializer objectSerializer() {
        return wrappedAppender.objectSerializer();
    }

    public void writeOrderedLong(long offset, long v) {
        wrappedAppender.writeOrderedLong(offset, v);
    }

    public long addAtomicLong(long offset, long l) {
        return wrappedAppender.addAtomicLong(offset, l);
    }

    @NotNull
    public ByteStringAppender append(char c) {
        return wrappedAppender.append(c);
    }

    public void busyLockInt(long offset) throws InterruptedException, IllegalStateException {
        wrappedAppender.busyLockInt(offset);
    }

    public void resetLockInt(long offset) {
        wrappedAppender.resetLockInt(offset);
    }

    @Nullable
    public String readLine() {
        return wrappedAppender.readLine();
    }

    public char readChar(long offset) {
        return wrappedAppender.readChar(offset);
    }

    @Nullable
    public <T> T readInstance(@NotNull Class<T> objClass, T obj) {
        return wrappedAppender.readInstance(objClass, obj);
    }

    @NotNull
    public ByteStringAppender append(boolean b) {
        return wrappedAppender.append(b);
    }

    public int addUnsignedByte(long offset, int i) {
        return wrappedAppender.addUnsignedByte(offset, i);
    }

    public boolean wasPadding() {
        return wrappedAppender.wasPadding();
    }

    public void readFully(@NotNull byte[] bytes, int off, int len) {
        wrappedAppender.readFully(bytes, off, len);
    }

    public void readFully(@NotNull char[] data, int off, int len) {
        wrappedAppender.readFully(data, off, len);
    }

    public int addAndGetInt(long offset, int delta) {
        return wrappedAppender.addAndGetInt(offset, delta);
    }

    public long index() {
        return wrappedAppender.index();
    }

    public long lastWrittenIndex() {
        return wrappedAppender.lastWrittenIndex();
    }

    public long addUnsignedInt(long offset, long i) {
        return wrappedAppender.addUnsignedInt(offset, i);
    }

    public void writeInt48(long v) {
        wrappedAppender.writeInt48(v);
    }

    @NotNull
    public ByteStringAppender append(@NotNull MutableDecimal md) {
        return wrappedAppender.append(md);
    }

    public <K, V> Map<K, V> readMap(@NotNull Map<K, V> map, @NotNull Class<K> kClass, @NotNull Class<V> vClass) {
        return wrappedAppender.readMap(map, kClass, vClass);
    }

    public char charAt(int index) {
        return wrappedAppender.charAt(index);
    }

    public void writeOrderedFloat(long offset, float v) {
        wrappedAppender.writeOrderedFloat(offset, v);
    }

    public void unlockRWWriteLock(long offset) throws IllegalStateException {
        wrappedAppender.unlockRWWriteLock(offset);
    }

    public void parseUTF(@NotNull StringBuilder builder, @NotNull StopCharTester tester) throws BufferUnderflowException {
        wrappedAppender.parseUTF(builder, tester);
    }

    @NotNull
    public InputStream inputStream() {
        return wrappedAppender.inputStream();
    }

    public long remaining() {
        return wrappedAppender.remaining();
    }

    public void writeByte(long offset, int b) {
        wrappedAppender.writeByte(offset, b);
    }

    public double readDouble() {
        return wrappedAppender.readDouble();
    }

    public int readCompactInt() {
        return wrappedAppender.readCompactInt();
    }

    public void release() {
        wrappedAppender.release();
    }

    public boolean readBoolean(long offset) {
        return wrappedAppender.readBoolean(offset);
    }

    public void writeBoolean(boolean v) {
        wrappedAppender.writeBoolean(v);
    }

    public int read(@NotNull byte[] bytes) {
        return wrappedAppender.read(bytes);
    }

    public void writeChars(@NotNull String s) {
        wrappedAppender.writeChars(s);
    }

    public void startExcerpt(long capacity) {
        wrappedAppender.startExcerpt(capacity);
    }

    public Bytes slice() {
        return wrappedAppender.slice();
    }

    public Bytes zeroOut() {
        return wrappedAppender.zeroOut();
    }

    public void toString(Appendable sb, long start, long position, long end) {
        wrappedAppender.toString(sb, start, position, end);
    }

    public void writeOrderedDouble(long offset, double v) {
        wrappedAppender.writeOrderedDouble(offset, v);
    }

    public long readStopBit() {
        return wrappedAppender.readStopBit();
    }

    public void busyLockLong(long offset) throws InterruptedException, IllegalStateException {
        wrappedAppender.busyLockLong(offset);
    }

    public void writeDouble(double v) {
        wrappedAppender.writeDouble(v);
    }

    public double readDouble(long offset) {
        return wrappedAppender.readDouble(offset);
    }

    public float addFloat(long offset, float f) {
        return wrappedAppender.addFloat(offset, f);
    }

    public boolean skipTo(@NotNull StopCharTester tester) {
        return wrappedAppender.skipTo(tester);
    }

    public void writeChar(int v) {
        wrappedAppender.writeChar(v);
    }

    public void writeInt(long offset, int v) {
        wrappedAppender.writeInt(offset, v);
    }

    @NotNull
    public OutputStream outputStream() {
        return wrappedAppender.outputStream();
    }

    public boolean compareAndSwapDouble(long offset, double expected, double x) {
        return wrappedAppender.compareAndSwapDouble(offset, expected, x);
    }

    public File file() {
        return wrappedAppender.file();
    }

    public <E> void readList(@NotNull Collection<E> list, @NotNull Class<E> eClass) {
        wrappedAppender.readList(list, eClass);
    }

    public void writeUnsignedByte(int v) {
        wrappedAppender.writeUnsignedByte(v);
    }

    public int readInt24(long offset) {
        return wrappedAppender.readInt24(offset);
    }

    public long readInt48() {
        return wrappedAppender.readInt48();
    }

    public void write(@NotNull char[] data) {
        wrappedAppender.write(data);
    }

    @Nullable
    public Object readObject() throws IllegalStateException {
        return wrappedAppender.readObject();
    }

    @NotNull
    public ByteStringAppender append(@net.openhft.lang.model.constraints.Nullable Enum value) {
        return wrappedAppender.append(value);
    }

    @NotNull
    public String parseUTF(@NotNull StopCharTester tester) throws BufferUnderflowException {
        return wrappedAppender.parseUTF(tester);
    }

    public int readInt() {
        return wrappedAppender.readInt();
    }

    public void write(@NotNull char[] data, int off, int len) {
        wrappedAppender.write(data, off, len);
    }

    public int addUnsignedShort(long offset, int i) {
        return wrappedAppender.addUnsignedShort(offset, i);
    }

    public float readFloat() {
        return wrappedAppender.readFloat();
    }

    public int available() {
        return wrappedAppender.available();
    }

    public long position() {
        return wrappedAppender.position();
    }

    public double addDouble(long offset, double d) {
        return wrappedAppender.addDouble(offset, d);
    }

    public void write(int b) {
        wrappedAppender.write(b);
    }

    public int skipBytes(int n) {
        return wrappedAppender.skipBytes(n);
    }

    public short readCompactShort() {
        return wrappedAppender.readCompactShort();
    }

    public void write(long offset, byte[] bytes) {
        wrappedAppender.write(offset, bytes);
    }

    public <E> void writeList(@NotNull Collection<E> list) {
        wrappedAppender.writeList(list);
    }

    public int read(@NotNull byte[] bytes, int off, int len) {
        return wrappedAppender.read(bytes, off, len);
    }

    public int readInt(long offset) {
        return wrappedAppender.readInt(offset);
    }

    public void writeFloat(long offset, float v) {
        wrappedAppender.writeFloat(offset, v);
    }

    public long parseLong() throws BufferUnderflowException {
        return wrappedAppender.parseLong();
    }

    public int readUnsignedByte(long offset) {
        return wrappedAppender.readUnsignedByte(offset);
    }

    public Bytes slice(long offset, long length) {
        return wrappedAppender.slice(offset, length);
    }

    public void writeObject(@Nullable Object object) {
        wrappedAppender.writeObject(object);
    }

    public int length() {
        return wrappedAppender.length();
    }

    public char readChar() {
        return wrappedAppender.readChar();
    }

    public int read() {
        return wrappedAppender.read();
    }

    public void writeBoolean(long offset, boolean v) {
        wrappedAppender.writeBoolean(offset, v);
    }

    public double parseDouble() throws BufferUnderflowException {
        return wrappedAppender.parseDouble();
    }

    public void nextSynchronous(boolean nextSynchronous) {
        wrappedAppender.nextSynchronous(nextSynchronous);
    }

    public void writeCompactDouble(double v) {
        wrappedAppender.writeCompactDouble(v);
    }

    public float addAtomicFloat(long offset, float f) {
        return wrappedAppender.addAtomicFloat(offset, f);
    }

    public void selfTerminating(boolean selfTerminate) {
        wrappedAppender.selfTerminating(selfTerminate);
    }

    public long readCompactUnsignedInt() {
        return wrappedAppender.readCompactUnsignedInt();
    }

    public double readVolatileDouble(long offset) {
        return wrappedAppender.readVolatileDouble(offset);
    }

    public long addLong(long offset, long i) {
        return wrappedAppender.addLong(offset, i);
    }

    public long readLong(long offset) {
        return wrappedAppender.readLong(offset);
    }

    public boolean compareAndSwapInt(long offset, int expected, int x) {
        return wrappedAppender.compareAndSwapInt(offset, expected, x);
    }

    @NotNull
    public ByteStringAppender append(@NotNull CharSequence s) {
        return wrappedAppender.append(s);
    }

    @NotNull
    public ByteStringAppender append(int i) {
        return wrappedAppender.append(i);
    }

    public <K, V> void writeMap(@NotNull Map<K, V> map) {
        wrappedAppender.writeMap(map);
    }

    public Boolean parseBoolean(@NotNull StopCharTester tester) throws BufferUnderflowException {
        return wrappedAppender.parseBoolean(tester);
    }

    public boolean tryRWReadLock(long offset, long timeOutNS) throws IllegalStateException {
        return wrappedAppender.tryRWReadLock(offset, timeOutNS);
    }

    public boolean nextSynchronous() {
        return wrappedAppender.nextSynchronous();
    }

    public int readUnsignedShort(long offset) {
        return wrappedAppender.readUnsignedShort(offset);
    }

    public void writeUTFΔ(long offset, int maxSize, @Nullable CharSequence s) throws IllegalStateException {
        wrappedAppender.writeUTFΔ(offset, maxSize, s);
    }

    public byte readByte(long offset) {
        return wrappedAppender.readByte(offset);
    }

    @NotNull
    public ByteStringAppender append(long l) {
        return wrappedAppender.append(l);
    }

    public void writeUTFΔ(@Nullable CharSequence s) {
        wrappedAppender.writeUTFΔ(s);
    }

    public boolean compareAndSwapLong(long offset, long expected, long x) {
        return wrappedAppender.compareAndSwapLong(offset, expected, x);
    }

    public void writeCompactShort(int v) {
        wrappedAppender.writeCompactShort(v);
    }

    public Bytes bytes() {
        return wrappedAppender.bytes();
    }

    public void write(byte[] bytes) {
        wrappedAppender.write(bytes);
    }

    public void unlockInt(long offset) throws IllegalMonitorStateException {
        wrappedAppender.unlockInt(offset);
    }

    public boolean tryLockLong(long offset) {
        return wrappedAppender.tryLockLong(offset);
    }

    public byte readByte() {
        return wrappedAppender.readByte();
    }

    public boolean tryRWWriteLock(long offset, long timeOutNS) throws IllegalStateException {
        return wrappedAppender.tryRWWriteLock(offset, timeOutNS);
    }

    public void write(byte[] bytes, int off, int len) {
        wrappedAppender.write(bytes, off, len);
    }

    public void writeUTF(@NotNull String s) {
        wrappedAppender.writeUTF(s);
    }

    public Bytes load() {
        return wrappedAppender.load();
    }

    public int getAndAdd(long offset, int delta) {
        return wrappedAppender.getAndAdd(offset, delta);
    }

    public short readShort(long offset) {
        return wrappedAppender.readShort(offset);
    }

    public boolean stepBackAndSkipTo(@NotNull StopCharTester tester) {
        return wrappedAppender.stepBackAndSkipTo(tester);
    }

    public void resetLockLong(long offset) {
        wrappedAppender.resetLockLong(offset);
    }

    public int readVolatileInt(long offset) {
        return wrappedAppender.readVolatileInt(offset);
    }

    @NotNull
    public ByteOrder byteOrder() {
        return wrappedAppender.byteOrder();
    }

    public Bytes bytes(long offset, long length) {
        return wrappedAppender.bytes(offset, length);
    }

    public void alignPositionAddr(int alignment) {
        wrappedAppender.alignPositionAddr(alignment);
    }

    public void writeUnsignedShort(int v) {
        wrappedAppender.writeUnsignedShort(v);
    }

    public long parseLong(int base) throws BufferUnderflowException {
        return wrappedAppender.parseLong(base);
    }

    public boolean readBoolean() {
        return wrappedAppender.readBoolean();
    }

    public void checkEndOfBuffer() throws IndexOutOfBoundsException {
        wrappedAppender.checkEndOfBuffer();
    }

    public float readVolatileFloat(long offset) {
        return wrappedAppender.readVolatileFloat(offset);
    }

    @NotNull
    public MutableDecimal parseDecimal(@NotNull MutableDecimal decimal) throws BufferUnderflowException {
        return wrappedAppender.parseDecimal(decimal);
    }

    public double addAtomicDouble(long offset, double d) {
        return wrappedAppender.addAtomicDouble(offset, d);
    }

    public void unlockLong(long offset) throws IllegalMonitorStateException {
        wrappedAppender.unlockLong(offset);
    }

    public void writeFloat(float v) {
        wrappedAppender.writeFloat(v);
    }

    public void reserve() {
        wrappedAppender.reserve();
    }

    public void write(@NotNull ByteBuffer bb) {
        wrappedAppender.write(bb);
    }

    public long threadIdForLockLong(long offset) {
        return wrappedAppender.threadIdForLockLong(offset);
    }

    public void writeChar(long offset, int v) {
        wrappedAppender.writeChar(offset, v);
    }

    public boolean tryLockNanosLong(long offset, long nanos) {
        return wrappedAppender.tryLockNanosLong(offset, nanos);
    }

    public int addAtomicInt(long offset, int i) {
        return wrappedAppender.addAtomicInt(offset, i);
    }

    public <OBJ> void writeInstance(@NotNull Class<OBJ> objClass, @NotNull OBJ obj) {
        wrappedAppender.writeInstance(objClass, obj);
    }

    public void readFully(@NotNull byte[] bytes) {
        wrappedAppender.readFully(bytes);
    }

    public Bytes position(long position) {
        return wrappedAppender.position(position);
    }

    public void writeLong(long offset, long v) {
        wrappedAppender.writeLong(offset, v);
    }

    public void readObject(Object object, int start, int end) {
        wrappedAppender.readObject(object, start, end);
    }

    public int threadIdForLockInt(long offset) {
        return wrappedAppender.threadIdForLockInt(offset);
    }

    @NotNull
    public ByteStringAppender appendDateMillis(long timeInMS) {
        return wrappedAppender.appendDateMillis(timeInMS);
    }

    public void writeInt24(int v) {
        wrappedAppender.writeInt24(v);
    }

    public boolean startsWith(RandomDataInput keyBytes) {
        return wrappedAppender.startsWith(keyBytes);
    }

    public long readUnsignedInt() {
        return wrappedAppender.readUnsignedInt();
    }

    public Bytes limit(long limit) {
        return wrappedAppender.limit(limit);
    }

    public void finish() {
        wrappedAppender.finish();
    }

    public long address() {
        return wrappedAppender.address();
    }

    public boolean tryLockInt(long offset) {
        return wrappedAppender.tryLockInt(offset);
    }

    public long readVolatileLong(long offset) {
        return wrappedAppender.readVolatileLong(offset);
    }

    public int readUnsignedByte() {
        return wrappedAppender.readUnsignedByte();
    }

    @Override
    public void readFully(long offset, @org.jetbrains.annotations.NotNull byte[] bytes, int off, int len) {
        wrappedAppender.readFully(offset, bytes, off, len);
    }

    @Override
    public void write(long offset, byte[] bytes, int off, int len) {
        throw new UnsupportedOperationException();
    }
}
