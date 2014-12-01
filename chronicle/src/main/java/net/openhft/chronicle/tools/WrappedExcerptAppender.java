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

public class WrappedExcerptAppender implements ExcerptAppender {
    protected final ExcerptAppender warappedAppender;

    public WrappedExcerptAppender(final @NotNull ExcerptAppender appender) {
        this.warappedAppender = appender;
    }

    public void startExcerpt() {
        warappedAppender.startExcerpt();
    }

    public ByteBuffer sliceAsByteBuffer(@Nullable ByteBuffer toReuse) {
        return warappedAppender.sliceAsByteBuffer(toReuse);
    }

    public void readMarshallable(@NotNull Bytes in) throws IllegalStateException {
        warappedAppender.readMarshallable(in);
    }

    public long readCompactLong() {
        return warappedAppender.readCompactLong();
    }

    public boolean tryLockNanosInt(long offset, long nanos) {
        return warappedAppender.tryLockNanosInt(offset, nanos);
    }

    public void writeMarshallable(@NotNull Bytes out) {
        warappedAppender.writeMarshallable(out);
    }

    public int readInt24() {
        return warappedAppender.readInt24();
    }

    public void flush() {
        warappedAppender.flush();
    }

    public void writeDouble(long offset, double v) {
        warappedAppender.writeDouble(offset, v);
    }

    public long limit() {
        return warappedAppender.limit();
    }

    @NotNull
    public ByteStringAppender appendTimeMillis(long timeInMS) {
        return warappedAppender.appendTimeMillis(timeInMS);
    }

    @Nullable
    public <E extends Enum<E>> E parseEnum(@NotNull Class<E> eClass, @NotNull StopCharTester tester) throws BufferUnderflowException {
        return warappedAppender.parseEnum(eClass, tester);
    }

    public int refCount() {
        return warappedAppender.refCount();
    }

    public void writeShort(long offset, int v) {
        warappedAppender.writeShort(offset, v);
    }

    public <E> void writeEnum(@Nullable E e) {
        warappedAppender.writeEnum(e);
    }

    @NotNull
    public <E> ByteStringAppender append(@NotNull Iterable<E> list, @NotNull CharSequence separator) {
        return warappedAppender.append(list, separator);
    }

    public void writeCompactUnsignedShort(int v) {
        warappedAppender.writeCompactUnsignedShort(v);
    }

    public long readVolatileLong() {
        return warappedAppender.readVolatileLong();
    }

    public void write(RandomDataInput bytes, long position, long length) {
        warappedAppender.write(bytes, position, length);
    }

    public void writeOrderedInt(int v) {
        warappedAppender.writeOrderedInt(v);
    }

    public boolean readUTFΔ(@NotNull StringBuilder stringBuilder) {
        return warappedAppender.readUTFΔ(stringBuilder);
    }

    public void writeInt48(long offset, long v) {
        warappedAppender.writeInt48(offset, v);
    }

    public long readLong() {
        return warappedAppender.readLong();
    }

    public void writeLong(long v) {
        warappedAppender.writeLong(v);
    }

    @NotNull
    public ByteStringAppender appendDateTimeMillis(long timeInMS) {
        return warappedAppender.appendDateTimeMillis(timeInMS);
    }

    @Nullable
    public <E> E readEnum(@NotNull Class<E> eClass) {
        return warappedAppender.readEnum(eClass);
    }

    public void write(RandomDataInput bytes) {
        warappedAppender.write(bytes);
    }

    @NotNull
    public ByteStringAppender append(double d) {
        return warappedAppender.append(d);
    }

    @NotNull
    public String toDebugString() {
        return warappedAppender.toDebugString();
    }

    public boolean isFinished() {
        return warappedAppender.isFinished();
    }

    public void writeCompactUnsignedInt(long v) {
        warappedAppender.writeCompactUnsignedInt(v);
    }

    @NotNull
    public ByteStringAppender append(double d, int precision) {
        return warappedAppender.append(d, precision);
    }

    public int readUnsignedByteOrThrow() throws BufferUnderflowException {
        return warappedAppender.readUnsignedByteOrThrow();
    }

    public Bytes zeroOut(long start, long end) {
        warappedAppender.zeroOut(start, end);
        return this;
    }

    @Override
    public Bytes zeroOut(long start, long end, boolean ifNotZero) {
        warappedAppender.zeroOut(start, end, ifNotZero);
        return this;
    }

    public void writeShort(int v) {
        warappedAppender.writeShort(v);
    }

    public short addShort(long offset, short s) {
        return warappedAppender.addShort(offset, s);
    }

    public void writeUnsignedInt(long v) {
        warappedAppender.writeUnsignedInt(v);
    }

    public void free() {
        warappedAppender.free();
    }

    public int readUnsignedShort() {
        return warappedAppender.readUnsignedShort();
    }

    public void writeStopBit(long n) {
        warappedAppender.writeStopBit(n);
    }

    @Nullable
    public <T> T readObject(Class<T> tClass) throws IllegalStateException {
        return warappedAppender.readObject(tClass);
    }

    public void writeCompactInt(int v) {
        warappedAppender.writeCompactInt(v);
    }

    public void writeOrderedLong(long v) {
        warappedAppender.writeOrderedLong(v);
    }

    public byte addByte(long offset, byte b) {
        return warappedAppender.addByte(offset, b);
    }

    public int readVolatileInt() {
        return warappedAppender.readVolatileInt();
    }

    public void close() {
        warappedAppender.close();
    }

    public void read(@NotNull ByteBuffer bb) {
        warappedAppender.read(bb);
    }

    @NotNull
    public ByteStringAppender append(long l, int base) {
        return warappedAppender.append(l, base);
    }

    public long skip(long n) {
        return warappedAppender.skip(n);
    }

    public boolean selfTerminating() {
        return warappedAppender.selfTerminating();
    }

    public void writeBytes(@NotNull String s) {
        warappedAppender.writeBytes(s);
    }

    public long size() {
        return warappedAppender.size();
    }

    public int readCompactUnsignedShort() {
        return warappedAppender.readCompactUnsignedShort();
    }

    @NotNull
    public ByteStringAppender append(@NotNull CharSequence s, int start, int end) {
        return warappedAppender.append(s, start, end);
    }

    public void writeCompactLong(long v) {
        warappedAppender.writeCompactLong(v);
    }

    public double readCompactDouble() {
        return warappedAppender.readCompactDouble();
    }

    public void writeOrderedInt(long offset, int v) {
        warappedAppender.writeOrderedInt(offset, v);
    }

    public void writeObject(Object object, int start, int end) {
        warappedAppender.writeObject(object, start, end);
    }

    public CharSequence asString() {
        return warappedAppender.asString();
    }

    @Nullable
    public String readUTFΔ() {
        return warappedAppender.readUTFΔ();
    }

    public Bytes flip() {
        return warappedAppender.flip();
    }

    public int addInt(long offset, int i) {
        return warappedAppender.addInt(offset, i);
    }

    public long readUnsignedInt(long offset) {
        return warappedAppender.readUnsignedInt(offset);
    }

    public void writeByte(int v) {
        warappedAppender.writeByte(v);
    }

    public void writeUnsignedInt(long offset, long v) {
        warappedAppender.writeUnsignedInt(offset, v);
    }

    public void addPaddedEntry() {
        warappedAppender.addPaddedEntry();
    }

    public void writeInt(int v) {
        warappedAppender.writeInt(v);
    }

    public short readShort() {
        return warappedAppender.readShort();
    }

    public Chronicle chronicle() {
        return warappedAppender.chronicle();
    }

    public void writeUnsignedByte(long offset, int v) {
        warappedAppender.writeUnsignedByte(offset, v);
    }

    public void asString(Appendable appendable) {
        warappedAppender.asString(appendable);
    }

    public long readInt48(long offset) {
        return warappedAppender.readInt48(offset);
    }

    public void unlockRWReadLock(long offset) throws IllegalStateException {
        warappedAppender.unlockRWReadLock(offset);
    }

    @NotNull
    public String readUTF() {
        return warappedAppender.readUTF();
    }

    public void writeUnsignedShort(long offset, int v) {
        warappedAppender.writeUnsignedShort(offset, v);
    }

    public void readFully(@NotNull char[] data) {
        warappedAppender.readFully(data);
    }

    public void writeInt24(long offset, int v) {
        warappedAppender.writeInt24(offset, v);
    }

    public void writeChars(@NotNull CharSequence cs) {
        warappedAppender.writeChars(cs);
    }

    public float readFloat(long offset) {
        return warappedAppender.readFloat(offset);
    }

    public long capacity() {
        return warappedAppender.capacity();
    }

    public CharSequence subSequence(int start, int end) {
        return warappedAppender.subSequence(start, end);
    }

    public Bytes clear() {
        return warappedAppender.clear();
    }

    @Nullable
    public String readUTFΔ(long offset) throws IllegalStateException {
        return warappedAppender.readUTFΔ(offset);
    }

    @NotNull
    public ObjectSerializer objectSerializer() {
        return warappedAppender.objectSerializer();
    }

    public void writeOrderedLong(long offset, long v) {
        warappedAppender.writeOrderedLong(offset, v);
    }

    public long addAtomicLong(long offset, long l) {
        return warappedAppender.addAtomicLong(offset, l);
    }

    @NotNull
    public ByteStringAppender append(char c) {
        return warappedAppender.append(c);
    }

    public void busyLockInt(long offset) throws InterruptedException, IllegalStateException {
        warappedAppender.busyLockInt(offset);
    }

    public void resetLockInt(long offset) {
        warappedAppender.resetLockInt(offset);
    }

    @Nullable
    public String readLine() {
        return warappedAppender.readLine();
    }

    public char readChar(long offset) {
        return warappedAppender.readChar(offset);
    }

    @Nullable
    public <T> T readInstance(@NotNull Class<T> objClass, T obj) {
        return warappedAppender.readInstance(objClass, obj);
    }

    @NotNull
    public ByteStringAppender append(boolean b) {
        return warappedAppender.append(b);
    }

    public int addUnsignedByte(long offset, int i) {
        return warappedAppender.addUnsignedByte(offset, i);
    }

    public boolean wasPadding() {
        return warappedAppender.wasPadding();
    }

    public void readFully(@NotNull byte[] bytes, int off, int len) {
        warappedAppender.readFully(bytes, off, len);
    }

    public void readFully(@NotNull char[] data, int off, int len) {
        warappedAppender.readFully(data, off, len);
    }

    public int addAndGetInt(long offset, int delta) {
        return warappedAppender.addAndGetInt(offset, delta);
    }

    public long index() {
        return warappedAppender.index();
    }

    public long lastWrittenIndex() {
        return warappedAppender.lastWrittenIndex();
    }

    public long addUnsignedInt(long offset, long i) {
        return warappedAppender.addUnsignedInt(offset, i);
    }

    public void writeInt48(long v) {
        warappedAppender.writeInt48(v);
    }

    @NotNull
    public ByteStringAppender append(@NotNull MutableDecimal md) {
        return warappedAppender.append(md);
    }

    public <K, V> Map<K, V> readMap(@NotNull Map<K, V> map, @NotNull Class<K> kClass, @NotNull Class<V> vClass) {
        return warappedAppender.readMap(map, kClass, vClass);
    }

    public char charAt(int index) {
        return warappedAppender.charAt(index);
    }

    public void writeOrderedFloat(long offset, float v) {
        warappedAppender.writeOrderedFloat(offset, v);
    }

    public void unlockRWWriteLock(long offset) throws IllegalStateException {
        warappedAppender.unlockRWWriteLock(offset);
    }

    public void parseUTF(@NotNull StringBuilder builder, @NotNull StopCharTester tester) throws BufferUnderflowException {
        warappedAppender.parseUTF(builder, tester);
    }

    @NotNull
    public InputStream inputStream() {
        return warappedAppender.inputStream();
    }

    @NotNull
    public ExcerptAppender toEnd() {
        return warappedAppender.toEnd();
    }

    public long remaining() {
        return warappedAppender.remaining();
    }

    public void writeByte(long offset, int b) {
        warappedAppender.writeByte(offset, b);
    }

    public double readDouble() {
        return warappedAppender.readDouble();
    }

    public int readCompactInt() {
        return warappedAppender.readCompactInt();
    }

    public void release() {
        warappedAppender.release();
    }

    public boolean readBoolean(long offset) {
        return warappedAppender.readBoolean(offset);
    }

    public void writeBoolean(boolean v) {
        warappedAppender.writeBoolean(v);
    }

    public int read(@NotNull byte[] bytes) {
        return warappedAppender.read(bytes);
    }

    public void writeChars(@NotNull String s) {
        warappedAppender.writeChars(s);
    }

    public void startExcerpt(long capacity) {
        warappedAppender.startExcerpt(capacity);
    }

    public Bytes slice() {
        return warappedAppender.slice();
    }

    public Bytes zeroOut() {
        return warappedAppender.zeroOut();
    }

    public void toString(Appendable sb, long start, long position, long end) {
        warappedAppender.toString(sb, start, position, end);
    }

    public void writeOrderedDouble(long offset, double v) {
        warappedAppender.writeOrderedDouble(offset, v);
    }

    public long readStopBit() {
        return warappedAppender.readStopBit();
    }

    public void busyLockLong(long offset) throws InterruptedException, IllegalStateException {
        warappedAppender.busyLockLong(offset);
    }

    public void writeDouble(double v) {
        warappedAppender.writeDouble(v);
    }

    public double readDouble(long offset) {
        return warappedAppender.readDouble(offset);
    }

    public float addFloat(long offset, float f) {
        return warappedAppender.addFloat(offset, f);
    }

    public boolean skipTo(@NotNull StopCharTester tester) {
        return warappedAppender.skipTo(tester);
    }

    public void writeChar(int v) {
        warappedAppender.writeChar(v);
    }

    public void writeInt(long offset, int v) {
        warappedAppender.writeInt(offset, v);
    }

    @NotNull
    public OutputStream outputStream() {
        return warappedAppender.outputStream();
    }

    public boolean compareAndSwapDouble(long offset, double expected, double x) {
        return warappedAppender.compareAndSwapDouble(offset, expected, x);
    }

    public File file() {
        return warappedAppender.file();
    }

    public <E> void readList(@NotNull Collection<E> list, @NotNull Class<E> eClass) {
        warappedAppender.readList(list, eClass);
    }

    public void writeUnsignedByte(int v) {
        warappedAppender.writeUnsignedByte(v);
    }

    public int readInt24(long offset) {
        return warappedAppender.readInt24(offset);
    }

    public long readInt48() {
        return warappedAppender.readInt48();
    }

    public void write(@NotNull char[] data) {
        warappedAppender.write(data);
    }

    @Nullable
    public Object readObject() throws IllegalStateException {
        return warappedAppender.readObject();
    }

    @NotNull
    public ByteStringAppender append(@net.openhft.lang.model.constraints.Nullable Enum value) {
        return warappedAppender.append(value);
    }

    @NotNull
    public String parseUTF(@NotNull StopCharTester tester) throws BufferUnderflowException {
        return warappedAppender.parseUTF(tester);
    }

    public int readInt() {
        return warappedAppender.readInt();
    }

    public void write(@NotNull char[] data, int off, int len) {
        warappedAppender.write(data, off, len);
    }

    public int addUnsignedShort(long offset, int i) {
        return warappedAppender.addUnsignedShort(offset, i);
    }

    public float readFloat() {
        return warappedAppender.readFloat();
    }

    public int available() {
        return warappedAppender.available();
    }

    public long position() {
        return warappedAppender.position();
    }

    public double addDouble(long offset, double d) {
        return warappedAppender.addDouble(offset, d);
    }

    public void write(int b) {
        warappedAppender.write(b);
    }

    public int skipBytes(int n) {
        return warappedAppender.skipBytes(n);
    }

    public short readCompactShort() {
        return warappedAppender.readCompactShort();
    }

    public void write(long offset, byte[] bytes) {
        warappedAppender.write(offset, bytes);
    }

    public <E> void writeList(@NotNull Collection<E> list) {
        warappedAppender.writeList(list);
    }

    public int read(@NotNull byte[] bytes, int off, int len) {
        return warappedAppender.read(bytes, off, len);
    }

    public int readInt(long offset) {
        return warappedAppender.readInt(offset);
    }

    public void writeFloat(long offset, float v) {
        warappedAppender.writeFloat(offset, v);
    }

    public long parseLong() throws BufferUnderflowException {
        return warappedAppender.parseLong();
    }

    public int readUnsignedByte(long offset) {
        return warappedAppender.readUnsignedByte(offset);
    }

    public Bytes slice(long offset, long length) {
        return warappedAppender.slice(offset, length);
    }

    public void writeObject(@Nullable Object object) {
        warappedAppender.writeObject(object);
    }

    public int length() {
        return warappedAppender.length();
    }

    public char readChar() {
        return warappedAppender.readChar();
    }

    public int read() {
        return warappedAppender.read();
    }

    public void writeBoolean(long offset, boolean v) {
        warappedAppender.writeBoolean(offset, v);
    }

    public double parseDouble() throws BufferUnderflowException {
        return warappedAppender.parseDouble();
    }

    public void nextSynchronous(boolean nextSynchronous) {
        warappedAppender.nextSynchronous(nextSynchronous);
    }

    public void writeCompactDouble(double v) {
        warappedAppender.writeCompactDouble(v);
    }

    public float addAtomicFloat(long offset, float f) {
        return warappedAppender.addAtomicFloat(offset, f);
    }

    public void selfTerminating(boolean selfTerminate) {
        warappedAppender.selfTerminating(selfTerminate);
    }

    public long readCompactUnsignedInt() {
        return warappedAppender.readCompactUnsignedInt();
    }

    public double readVolatileDouble(long offset) {
        return warappedAppender.readVolatileDouble(offset);
    }

    public long addLong(long offset, long i) {
        return warappedAppender.addLong(offset, i);
    }

    public long readLong(long offset) {
        return warappedAppender.readLong(offset);
    }

    public boolean compareAndSwapInt(long offset, int expected, int x) {
        return warappedAppender.compareAndSwapInt(offset, expected, x);
    }

    @NotNull
    public ByteStringAppender append(@NotNull CharSequence s) {
        return warappedAppender.append(s);
    }

    @NotNull
    public ByteStringAppender append(int i) {
        return warappedAppender.append(i);
    }

    public <K, V> void writeMap(@NotNull Map<K, V> map) {
        warappedAppender.writeMap(map);
    }

    public Boolean parseBoolean(@NotNull StopCharTester tester) throws BufferUnderflowException {
        return warappedAppender.parseBoolean(tester);
    }

    public boolean tryRWReadLock(long offset, long timeOutNS) throws IllegalStateException {
        return warappedAppender.tryRWReadLock(offset, timeOutNS);
    }

    public boolean nextSynchronous() {
        return warappedAppender.nextSynchronous();
    }

    public int readUnsignedShort(long offset) {
        return warappedAppender.readUnsignedShort(offset);
    }

    public void writeUTFΔ(long offset, int maxSize, @Nullable CharSequence s) throws IllegalStateException {
        warappedAppender.writeUTFΔ(offset, maxSize, s);
    }

    public byte readByte(long offset) {
        return warappedAppender.readByte(offset);
    }

    @NotNull
    public ByteStringAppender append(long l) {
        return warappedAppender.append(l);
    }

    public void writeUTFΔ(@Nullable CharSequence s) {
        warappedAppender.writeUTFΔ(s);
    }

    public boolean compareAndSwapLong(long offset, long expected, long x) {
        return warappedAppender.compareAndSwapLong(offset, expected, x);
    }

    public void writeCompactShort(int v) {
        warappedAppender.writeCompactShort(v);
    }

    public Bytes bytes() {
        return warappedAppender.bytes();
    }

    public void write(byte[] bytes) {
        warappedAppender.write(bytes);
    }

    public void unlockInt(long offset) throws IllegalMonitorStateException {
        warappedAppender.unlockInt(offset);
    }

    public boolean tryLockLong(long offset) {
        return warappedAppender.tryLockLong(offset);
    }

    public byte readByte() {
        return warappedAppender.readByte();
    }

    public boolean tryRWWriteLock(long offset, long timeOutNS) throws IllegalStateException {
        return warappedAppender.tryRWWriteLock(offset, timeOutNS);
    }

    public void write(byte[] bytes, int off, int len) {
        warappedAppender.write(bytes, off, len);
    }

    public void writeUTF(@NotNull String s) {
        warappedAppender.writeUTF(s);
    }

    public Bytes load() {
        return warappedAppender.load();
    }

    public int getAndAdd(long offset, int delta) {
        return warappedAppender.getAndAdd(offset, delta);
    }

    public short readShort(long offset) {
        return warappedAppender.readShort(offset);
    }

    public boolean stepBackAndSkipTo(@NotNull StopCharTester tester) {
        return warappedAppender.stepBackAndSkipTo(tester);
    }

    public void resetLockLong(long offset) {
        warappedAppender.resetLockLong(offset);
    }

    public int readVolatileInt(long offset) {
        return warappedAppender.readVolatileInt(offset);
    }

    @NotNull
    public ByteOrder byteOrder() {
        return warappedAppender.byteOrder();
    }

    public Bytes bytes(long offset, long length) {
        return warappedAppender.bytes(offset, length);
    }

    public void alignPositionAddr(int alignment) {
        warappedAppender.alignPositionAddr(alignment);
    }

    public void writeUnsignedShort(int v) {
        warappedAppender.writeUnsignedShort(v);
    }

    public long parseLong(int base) throws BufferUnderflowException {
        return warappedAppender.parseLong(base);
    }

    public boolean readBoolean() {
        return warappedAppender.readBoolean();
    }

    public void checkEndOfBuffer() throws IndexOutOfBoundsException {
        warappedAppender.checkEndOfBuffer();
    }

    public float readVolatileFloat(long offset) {
        return warappedAppender.readVolatileFloat(offset);
    }

    @NotNull
    public MutableDecimal parseDecimal(@NotNull MutableDecimal decimal) throws BufferUnderflowException {
        return warappedAppender.parseDecimal(decimal);
    }

    public double addAtomicDouble(long offset, double d) {
        return warappedAppender.addAtomicDouble(offset, d);
    }

    public void unlockLong(long offset) throws IllegalMonitorStateException {
        warappedAppender.unlockLong(offset);
    }

    public void writeFloat(float v) {
        warappedAppender.writeFloat(v);
    }

    public void reserve() {
        warappedAppender.reserve();
    }

    public void write(@NotNull ByteBuffer bb) {
        warappedAppender.write(bb);
    }

    public long threadIdForLockLong(long offset) {
        return warappedAppender.threadIdForLockLong(offset);
    }

    public void writeChar(long offset, int v) {
        warappedAppender.writeChar(offset, v);
    }

    public boolean tryLockNanosLong(long offset, long nanos) {
        return warappedAppender.tryLockNanosLong(offset, nanos);
    }

    public int addAtomicInt(long offset, int i) {
        return warappedAppender.addAtomicInt(offset, i);
    }

    public <OBJ> void writeInstance(@NotNull Class<OBJ> objClass, @NotNull OBJ obj) {
        warappedAppender.writeInstance(objClass, obj);
    }

    public void readFully(@NotNull byte[] bytes) {
        warappedAppender.readFully(bytes);
    }

    public Bytes position(long position) {
        return warappedAppender.position(position);
    }

    public void writeLong(long offset, long v) {
        warappedAppender.writeLong(offset, v);
    }

    public void readObject(Object object, int start, int end) {
        warappedAppender.readObject(object, start, end);
    }

    public int threadIdForLockInt(long offset) {
        return warappedAppender.threadIdForLockInt(offset);
    }

    @NotNull
    public ByteStringAppender appendDateMillis(long timeInMS) {
        return warappedAppender.appendDateMillis(timeInMS);
    }

    public void writeInt24(int v) {
        warappedAppender.writeInt24(v);
    }

    public boolean startsWith(RandomDataInput keyBytes) {
        return warappedAppender.startsWith(keyBytes);
    }

    public long readUnsignedInt() {
        return warappedAppender.readUnsignedInt();
    }

    public Bytes limit(long limit) {
        return warappedAppender.limit(limit);
    }

    public void finish() {
        warappedAppender.finish();
    }

    public long address() {
        return warappedAppender.address();
    }

    public boolean tryLockInt(long offset) {
        return warappedAppender.tryLockInt(offset);
    }

    public long readVolatileLong(long offset) {
        return warappedAppender.readVolatileLong(offset);
    }

    public int readUnsignedByte() {
        return warappedAppender.readUnsignedByte();
    }
}
