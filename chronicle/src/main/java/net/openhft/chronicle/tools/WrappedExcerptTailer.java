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
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.lang.io.*;
import net.openhft.lang.io.serialization.ObjectSerializer;
import net.openhft.lang.model.constraints.NotNull;
import net.openhft.lang.model.constraints.Nullable;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.Map;

public class WrappedExcerptTailer implements ExcerptTailer {
    protected ExcerptTailer wrappedTailer;

    public WrappedExcerptTailer(@NotNull ExcerptTailer tailer) {
        this.wrappedTailer = tailer;
    }

    @Override
    public String toDebugString(long limit) {
        return wrappedTailer.toDebugString(limit);
    }

    @Override
    public boolean compare(long offset, RandomDataInput input, long inputOffset, long len) {
        return wrappedTailer.compare(offset, input, inputOffset, len);
    }

    @Override
    public <E extends Enum<E>> E parseEnum(@NotNull Class<E> eClass, @NotNull StopCharTester tester) {
        return wrappedTailer.parseEnum(eClass, tester);
    }

    @Override
    public WrappedExcerptTailer clear() {
        wrappedTailer.clear();
        return this;
    }

    @Override
    public void readObject(Object object, int start, int end) {
        wrappedTailer.readObject(object, start, end);
    }

    @Override
    public void writeObject(Object object, int start, int end) {
        wrappedTailer.writeObject(object, start, end);
    }

    @Override
    public Chronicle chronicle() {
        return wrappedTailer.chronicle();
    }

    @Override
    public long size() {
        return wrappedTailer.size();
    }

    @Override
    public boolean nextIndex() {
        return wrappedTailer.nextIndex();
    }

    @Override
    public boolean index(long index) throws IndexOutOfBoundsException {
        return wrappedTailer.index(index);
    }

    @Override
    public void finish() {
        wrappedTailer.finish();
    }

    @Override
    public long index() {
        return wrappedTailer.index();
    }

    @Override
    public long position() {
        return wrappedTailer.position();
    }

    @Override
    public Boolean parseBoolean(@NotNull StopCharTester tester) {
        return wrappedTailer.parseBoolean(tester);
    }

    @Override
    public long capacity() {
        return wrappedTailer.capacity();
    }

    @Override
    public long remaining() {
        return wrappedTailer.remaining();
    }

    @Override
    public void readFully(@NotNull byte[] bytes) {
        wrappedTailer.readFully(bytes);
    }

    @Override
    public int skipBytes(int n) {
        return wrappedTailer.skipBytes(n);
    }

    @Override
    public void readFully(@NotNull byte[] b, int off, int len) {
        wrappedTailer.readFully(b, off, len);
    }

    @Override
    public boolean readBoolean() {
        return wrappedTailer.readBoolean();
    }

    @Override
    public boolean readBoolean(long offset) {
        return wrappedTailer.readBoolean(offset);
    }

    @Override
    public int readUnsignedByte() {
        return wrappedTailer.readUnsignedByte();
    }

    @Override
    public int readUnsignedByte(long offset) {
        return wrappedTailer.readUnsignedByte(offset);
    }

    @Override
    public int readUnsignedShort() {
        return wrappedTailer.readUnsignedShort();
    }

    @Override
    public int readUnsignedShort(long offset) {
        return wrappedTailer.readUnsignedShort(offset);
    }

    @Override
    public String readLine() {
        return wrappedTailer.readLine();
    }

    @NotNull
    @Override
    public String readUTF() {
        return wrappedTailer.readUTF();
    }

    @Nullable
    @Override
    public String readUTFΔ() {
        return wrappedTailer.readUTFΔ();
    }

    @Nullable
    @Override
    public String readUTFΔ(long offset) throws IllegalStateException {
        return wrappedTailer.readUTFΔ(offset);
    }

    @Override
    public boolean readUTFΔ(@NotNull StringBuilder stringBuilder) {
        return wrappedTailer.readUTFΔ(stringBuilder);
    }

    @NotNull
    @Override
    public String parseUTF(@NotNull StopCharTester tester) {
        return wrappedTailer.parseUTF(tester);
    }

    @Override
    public void parseUTF(@NotNull StringBuilder builder, @NotNull StopCharTester tester) {
        wrappedTailer.parseUTF(builder, tester);
    }

    @Override
    public short readCompactShort() {
        return wrappedTailer.readCompactShort();
    }

    @Override
    public int readCompactUnsignedShort() {
        return wrappedTailer.readCompactUnsignedShort();
    }

    @Override
    public int readInt24() {
        return wrappedTailer.readInt24();
    }

    @Override
    public int readInt24(long offset) {
        return wrappedTailer.readInt24(offset);
    }

    @Override
    public long readUnsignedInt() {
        return wrappedTailer.readUnsignedInt();
    }

    @Override
    public long readUnsignedInt(long offset) {
        return wrappedTailer.readUnsignedInt(offset);
    }

    @Override
    public int readCompactInt() {
        return wrappedTailer.readCompactInt();
    }

    @Override
    public long readCompactUnsignedInt() {
        return wrappedTailer.readCompactUnsignedInt();
    }

    @Override
    public long readInt48() {
        return wrappedTailer.readInt48();
    }

    @Override
    public long readInt48(long offset) {
        return wrappedTailer.readInt48(offset);
    }

    @Override
    public long readCompactLong() {
        return wrappedTailer.readCompactLong();
    }

    @Override
    public long readStopBit() {
        return wrappedTailer.readStopBit();
    }

    @Override
    public double readCompactDouble() {
        return wrappedTailer.readCompactDouble();
    }

    @Override
    public void read(@NotNull ByteBuffer bb) {
        wrappedTailer.read(bb);
    }

    @Override
    public void write(byte[] bytes) {
        wrappedTailer.write(bytes);
    }

    @Override
    public void write(char[] bytes) {
        wrappedTailer.write(bytes);
    }

    @Override
    public void write(@NotNull char[] data, int off, int len) {
        wrappedTailer.write(data, off, len);
    }

    @Override
    public ByteBuffer sliceAsByteBuffer(@Nullable ByteBuffer toReuse) {
        return wrappedTailer.sliceAsByteBuffer(toReuse);
    }

    @Override
    public void readFully(@NotNull char[] data) {
        wrappedTailer.readFully(data);
    }

    @Override
    public void readFully(@NotNull char[] data, int off, int len) {
        wrappedTailer.readFully(data, off, len);
    }

    @Override
    public void writeChars(@NotNull CharSequence cs) {
        wrappedTailer.writeChars(cs);
    }

    @Override
    public void writeBoolean(boolean v) {
        wrappedTailer.writeBoolean(v);
    }

    @Override
    public void writeBoolean(long offset, boolean v) {
        wrappedTailer.writeBoolean(offset, v);
    }

    @Override
    public void writeBytes(@NotNull String s) {
        wrappedTailer.writeBytes(s);
    }

    @Override
    public void writeChars(@NotNull String s) {
        wrappedTailer.writeChars(s);
    }

    @Override
    public void writeUTF(@NotNull String s) {
        wrappedTailer.writeUTF(s);
    }

    @Override
    public void writeUTFΔ(CharSequence str) {
        wrappedTailer.writeUTFΔ(str);
    }

    @Override
    public void writeUTFΔ(long offset, int maxSize, @Nullable CharSequence s) throws IllegalStateException {
        wrappedTailer.writeUTFΔ(offset, maxSize, s);
    }

    @Override
    public void writeByte(int v) {
        wrappedTailer.writeByte(v);
    }

    @Override
    public void writeUnsignedByte(int v) {
        wrappedTailer.writeUnsignedByte(v);
    }

    @Override
    public void writeUnsignedByte(long offset, int v) {
        wrappedTailer.writeUnsignedByte(offset, v);
    }

    @Override
    public void write(long offset, byte[] bytes) {
        wrappedTailer.write(offset, bytes);
    }

    @Override
    public void write(byte[] bytes, int off, int len) {
        wrappedTailer.write(bytes, off, len);
    }

    @Override
    public void writeUnsignedShort(int v) {
        wrappedTailer.writeUnsignedShort(v);
    }

    @Override
    public void writeUnsignedShort(long offset, int v) {
        wrappedTailer.writeUnsignedShort(offset, v);
    }

    @Override
    public void writeCompactShort(int v) {
        wrappedTailer.writeCompactShort(v);
    }

    @Override
    public void writeCompactUnsignedShort(int v) {
        wrappedTailer.writeCompactUnsignedShort(v);
    }

    @Override
    public void writeInt24(int v) {
        wrappedTailer.writeInt24(v);
    }

    @Override
    public void writeInt24(long offset, int v) {
        wrappedTailer.writeInt24(offset, v);
    }

    @Override
    public void writeUnsignedInt(long v) {
        wrappedTailer.writeUnsignedInt(v);
    }

    @Override
    public void writeUnsignedInt(long offset, long v) {
        wrappedTailer.writeUnsignedInt(offset, v);
    }

    @Override
    public void writeCompactInt(int v) {
        wrappedTailer.writeCompactInt(v);
    }

    @Override
    public void writeCompactUnsignedInt(long v) {
        wrappedTailer.writeCompactUnsignedInt(v);
    }

    @Override
    public void writeInt48(long v) {
        wrappedTailer.writeInt48(v);
    }

    @Override
    public void writeInt48(long offset, long v) {
        wrappedTailer.writeInt48(offset, v);
    }

    @Override
    public void writeCompactLong(long v) {
        wrappedTailer.writeCompactLong(v);
    }

    @Override
    public void writeCompactDouble(double v) {
        wrappedTailer.writeCompactDouble(v);
    }

    @Override
    public void write(@NotNull ByteBuffer bb) {
        wrappedTailer.write(bb);
    }

    @NotNull
    @Override
    public ByteStringAppender append(@NotNull CharSequence s) {
        wrappedTailer.append(s);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender append(@NotNull CharSequence s, int start, int end) {
        wrappedTailer.append(s, start, end);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender append(@Nullable Enum value) {
        wrappedTailer.append(value);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender append(boolean b) {
        wrappedTailer.append(b);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender append(char c) {
        wrappedTailer.append(c);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender append(int num) {
        wrappedTailer.append(num);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender append(long num) {
        wrappedTailer.append(num);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender append(double d) {
        wrappedTailer.append(d);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender append(double d, int precision) {
        wrappedTailer.append(d, precision);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender append(@NotNull MutableDecimal md) {
        wrappedTailer.append(md);
        return this;
    }

    @Override
    public double parseDouble() {
        return wrappedTailer.parseDouble();
    }

    @Override
    public long parseLong() {
        return wrappedTailer.parseLong();
    }

    @NotNull
    @Override
    public InputStream inputStream() {
        return wrappedTailer.inputStream();
    }

    @NotNull
    @Override
    public OutputStream outputStream() {
        return wrappedTailer.outputStream();
    }

    @Override
    public ObjectSerializer objectSerializer() {
        return wrappedTailer.objectSerializer();
    }

    @Override
    public <E> void writeEnum(E o) {
        wrappedTailer.writeEnum(o);
    }

    @Override
    public <E> E readEnum(@NotNull Class<E> aClass) {
        return wrappedTailer.readEnum(aClass);
    }

    @Override
    public <K, V> void writeMap(@NotNull Map<K, V> map) {
        wrappedTailer.writeMap(map);
    }

    @Override
    public <K, V> Map<K, V> readMap(@NotNull Map<K, V> map, @NotNull Class<K> kClass, @NotNull Class<V> vClass) {
        return wrappedTailer.readMap(map, kClass, vClass);
    }

    @Override
    public byte readByte() {
        return wrappedTailer.readByte();
    }

    @Override
    public byte readByte(long offset) {
        return wrappedTailer.readByte(offset);
    }

    @Override
    public short readShort() {
        return wrappedTailer.readShort();
    }

    @Override
    public short readShort(long offset) {
        return wrappedTailer.readShort(offset);
    }

    @Override
    public char readChar() {
        return wrappedTailer.readChar();
    }

    @Override
    public char readChar(long offset) {
        return wrappedTailer.readChar(offset);
    }

    @Override
    public int readInt() {
        return wrappedTailer.readInt();
    }

    @Override
    public int readInt(long offset) {
        return wrappedTailer.readInt(offset);
    }

    @Override
    public long readLong() {
        return wrappedTailer.readLong();
    }

    @Override
    public long readLong(long offset) {
        return wrappedTailer.readLong(offset);
    }

    @Override
    public float readFloat() {
        return wrappedTailer.readFloat();
    }

    @Override
    public float readFloat(long offset) {
        return wrappedTailer.readFloat(offset);
    }

    @Override
    public double readDouble() {
        return wrappedTailer.readDouble();
    }

    @Override
    public double readDouble(long offset) {
        return wrappedTailer.readDouble(offset);
    }

    @Override
    public void write(int b) {
        wrappedTailer.write(b);
    }

    @Override
    public void writeByte(long offset, int b) {
        wrappedTailer.writeByte(offset, b);
    }

    @Override
    public void writeShort(int v) {
        wrappedTailer.writeShort(v);
    }

    @Override
    public void writeShort(long offset, int v) {
        wrappedTailer.writeShort(offset, v);
    }

    @Override
    public void writeChar(int v) {
        wrappedTailer.writeChar(v);
    }

    @Override
    public void writeChar(long offset, int v) {
        wrappedTailer.writeChar(offset, v);
    }

    @Override
    public void writeInt(int v) {
        wrappedTailer.writeInt(v);
    }

    @Override
    public void writeInt(long offset, int v) {
        wrappedTailer.writeInt(offset, v);
    }

    @Override
    public void writeLong(long v) {
        wrappedTailer.writeLong(v);
    }

    @Override
    public void writeLong(long offset, long v) {
        wrappedTailer.writeLong(offset, v);
    }

    @Override
    public void writeStopBit(long n) {
        wrappedTailer.writeStopBit(n);
    }

    @Override
    public void writeFloat(float v) {
        wrappedTailer.writeFloat(v);
    }

    @Override
    public void writeFloat(long offset, float v) {
        wrappedTailer.writeFloat(offset, v);
    }

    @Override
    public void writeDouble(double v) {
        wrappedTailer.writeDouble(v);
    }

    @Override
    public void writeDouble(long offset, double v) {
        wrappedTailer.writeDouble(offset, v);
    }

    @Nullable
    @Override
    public Object readObject() {
        return wrappedTailer.readObject();
    }

    @Override
    public <T> T readObject(Class<T> tClass) throws IllegalStateException {
        return wrappedTailer.readObject(tClass);
    }

    @Override
    public int read() {
        return wrappedTailer.read();
    }

    @Override
    public int read(@NotNull byte[] bytes) {
        return wrappedTailer.read(bytes);
    }

    @Override
    public int read(@NotNull byte[] bytes, int off, int len) {
        return wrappedTailer.read(bytes, off, len);
    }

    @Override
    public long skip(long n) {
        return wrappedTailer.skip(n);
    }

    @Override
    public int available() {
        return wrappedTailer.available();
    }

    @Override
    public void close() {
        try {
            wrappedTailer.close();
        } catch (Exception ignored) {
        }
    }

    @Override
    public void writeObject(Object obj) {
        wrappedTailer.writeObject(obj);
    }

    @Override
    public void flush() {
        wrappedTailer.flush();
    }

    @Override
    public <E> void writeList(@NotNull Collection<E> list) {
        wrappedTailer.writeList(list);
    }

    @Override
    public <E> void readList(@NotNull Collection<E> list, @NotNull Class<E> eClass) {
        wrappedTailer.readList(list, eClass);
    }

    @Override
    public boolean stepBackAndSkipTo(@NotNull StopCharTester tester) {
        return wrappedTailer.stepBackAndSkipTo(tester);
    }

    @Override
    public boolean skipTo(@NotNull StopCharTester tester) {
        return wrappedTailer.skipTo(tester);
    }

    @NotNull
    @Override
    public MutableDecimal parseDecimal(@NotNull MutableDecimal decimal) {
        return wrappedTailer.parseDecimal(decimal);
    }

    @NotNull
    @Override
    public ExcerptTailer toStart() {
        wrappedTailer.toStart();
        return this;
    }

    @NotNull
    @Override
    public ExcerptTailer toEnd() {
        wrappedTailer.toEnd();
        return this;
    }

    @Override
    public boolean isFinished() {
        return wrappedTailer.isFinished();
    }

    @Override
    public boolean wasPadding() {
        return wrappedTailer.wasPadding();
    }

    @NotNull
    @Override
    public ByteStringAppender appendTimeMillis(long timeInMS) {
        wrappedTailer.appendTimeMillis(timeInMS);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender appendDateMillis(long timeInMS) {
        wrappedTailer.appendDateMillis(timeInMS);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender appendDateTimeMillis(long timeInMS) {
        wrappedTailer.appendDateTimeMillis(timeInMS);
        return this;
    }

    @NotNull
    @Override
    public <E> ByteStringAppender append(@NotNull Iterable<E> list, @NotNull CharSequence seperator) {
        wrappedTailer.append(list, seperator);
        return this;
    }

    @Override
    public int readVolatileInt() {
        return wrappedTailer.readVolatileInt();
    }

    @Override
    public int readVolatileInt(long offset) {
        return wrappedTailer.readVolatileInt(offset);
    }

    @Override
    public long readVolatileLong() {
        return wrappedTailer.readVolatileLong();
    }

    @Override
    public long readVolatileLong(long offset) {
        return wrappedTailer.readVolatileLong(offset);
    }

    @Override
    public void writeOrderedInt(int v) {
        wrappedTailer.writeOrderedInt(v);
    }

    @Override
    public void writeOrderedInt(long offset, int v) {
        wrappedTailer.writeOrderedInt(offset, v);
    }

    @Override
    public boolean compareAndSwapInt(long offset, int expected, int x) {
        return wrappedTailer.compareAndSwapInt(offset, expected, x);
    }

    @Override
    public int getAndAdd(long offset, int delta) {
        return wrappedTailer.getAndAdd(offset, delta);
    }

    @Override
    public int addAndGetInt(long offset, int delta) {
        return wrappedTailer.addAndGetInt(offset, delta);
    }

    @Override
    public void writeOrderedLong(long v) {
        wrappedTailer.writeOrderedLong(v);
    }

    @Override
    public void writeOrderedLong(long offset, long v) {
        wrappedTailer.writeOrderedLong(offset, v);
    }

    @Override
    public boolean compareAndSwapLong(long offset, long expected, long x) {
        return wrappedTailer.compareAndSwapLong(offset, expected, x);
    }

    @Override
    public WrappedExcerptTailer position(long position) {
        wrappedTailer.position(position);
        return this;
    }

    @NotNull
    @Override
    public ByteOrder byteOrder() {
        return wrappedTailer.byteOrder();
    }

    @Override
    public void checkEndOfBuffer() throws IndexOutOfBoundsException {
        wrappedTailer.checkEndOfBuffer();
    }

    @Override
    public boolean tryLockInt(long offset) {
        return wrappedTailer.tryLockInt(offset);
    }

    @Override
    public boolean tryLockNanosInt(long offset, long nanos) {
        return wrappedTailer.tryLockNanosInt(offset, nanos);
    }

    @Override
    public void busyLockInt(long offset) throws InterruptedException, IllegalStateException {
        wrappedTailer.busyLockInt(offset);
    }

    @Override
    public void unlockInt(long offset) throws IllegalStateException {
        wrappedTailer.unlockInt(offset);
    }

    @Override
    public boolean tryLockLong(long offset) {
        return wrappedTailer.tryLockLong(offset);
    }

    @Override
    public boolean tryLockNanosLong(long offset, long nanos) {
        return wrappedTailer.tryLockNanosLong(offset, nanos);
    }

    @Override
    public void busyLockLong(long offset) throws InterruptedException, IllegalStateException {
        wrappedTailer.busyLockLong(offset);
    }

    @Override
    public void unlockLong(long offset) throws IllegalStateException {
        wrappedTailer.unlockLong(offset);
    }

    @Override
    public float readVolatileFloat(long offset) {
        return wrappedTailer.readVolatileFloat(offset);
    }

    @Override
    public double readVolatileDouble(long offset) {
        return wrappedTailer.readVolatileDouble(offset);
    }

    @Override
    public void writeOrderedFloat(long offset, float v) {
        wrappedTailer.writeOrderedFloat(offset, v);
    }

    @Override
    public void writeOrderedDouble(long offset, double v) {
        wrappedTailer.writeOrderedDouble(offset, v);
    }

    @Override
    public byte addByte(long offset, byte b) {
        return wrappedTailer.addByte(offset, b);
    }

    @Override
    public int addUnsignedByte(long offset, int i) {
        return wrappedTailer.addUnsignedByte(offset, i);
    }

    @Override
    public short addShort(long offset, short s) {
        return wrappedTailer.addShort(offset, s);
    }

    @Override
    public int addUnsignedShort(long offset, int i) {
        return wrappedTailer.addUnsignedShort(offset, i);
    }

    @Override
    public int addInt(long offset, int i) {
        return wrappedTailer.addInt(offset, i);
    }

    @Override
    public long addUnsignedInt(long offset, long i) {
        return wrappedTailer.addUnsignedInt(offset, i);
    }

    @Override
    public long addLong(long offset, long i) {
        return wrappedTailer.addLong(offset, i);
    }

    @Override
    public float addFloat(long offset, float f) {
        return wrappedTailer.addFloat(offset, f);
    }

    @Override
    public double addDouble(long offset, double d) {
        return wrappedTailer.addDouble(offset, d);
    }

    @Override
    public int addAtomicInt(long offset, int i) {
        return wrappedTailer.addAtomicInt(offset, i);
    }

    @Override
    public long addAtomicLong(long offset, long l) {
        return wrappedTailer.addAtomicLong(offset, l);
    }

    @Override
    public float addAtomicFloat(long offset, float f) {
        return wrappedTailer.addAtomicFloat(offset, f);
    }

    @Override
    public double addAtomicDouble(long offset, double d) {
        return wrappedTailer.addAtomicDouble(offset, d);
    }

    @NotNull
    @Override
    public ByteStringAppender append(long l, int base) {
        wrappedTailer.append(l, base);
        return this;
    }

    @Override
    public long parseLong(int base) {
        return wrappedTailer.parseLong(base);
    }

    @Override
    public void write(RandomDataInput bytes, long position, long length) {
        wrappedTailer.write(bytes, position, length);
    }

    @Override
    public void readMarshallable(@NotNull Bytes in) throws IllegalStateException {
        wrappedTailer.readMarshallable(in);
    }

    @Override
    public void writeMarshallable(@NotNull Bytes out) {
        wrappedTailer.writeMarshallable(out);
    }

    @Override
    public int length() {
        return wrappedTailer.length();
    }

    @Override
    public char charAt(int index) {
        return wrappedTailer.charAt(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return wrappedTailer.subSequence(start, end);
    }

    @Override
    public Bytes flip() {
        return wrappedTailer.flip();
    }

    @NotNull
    @Override
    public <T> T readInstance(@NotNull Class<T> objClass, T obj) {
        return wrappedTailer.readInstance(objClass, obj);
    }

    @Override
    public boolean startsWith(RandomDataInput keyBytes) {
        return wrappedTailer.startsWith(keyBytes);
    }

    @Override
    public void write(RandomDataInput bytes) {
        wrappedTailer.write(bytes);
    }

    @Override
    public <OBJ> void writeInstance(@NotNull Class<OBJ> objClass, @NotNull OBJ obj) {
        wrappedTailer.writeInstance(objClass, obj);
    }

    @Override
    public Bytes zeroOut() {
        wrappedTailer.zeroOut();
        return this;
    }

    @Override
    public Bytes zeroOut(long start, long end) {
        wrappedTailer.zeroOut(start, end);
        return this;
    }

    @Override
    public Bytes zeroOut(long start, long end, boolean ifNotZero) {
        wrappedTailer.zeroOut(start, end, ifNotZero);
        return this;
    }

    @Override
    public long limit() {
        return wrappedTailer.limit();
    }

    @Override
    public Bytes limit(long limit) {
        wrappedTailer.limit(limit);
        return this;
    }

    @Override
    public Bytes load() {
        wrappedTailer.load();
        return this;
    }

    @Override
    public Bytes slice() {
        return wrappedTailer.slice();
    }

    @Override
    public Bytes slice(long offset, long length) {
        return wrappedTailer.slice(offset, length);
    }

    @Override
    public Bytes bytes() {
        return wrappedTailer.bytes();
    }

    @Override
    public Bytes bytes(long offset, long length) {
        return wrappedTailer.bytes(offset, length);
    }

    @Override
    public long address() {
        return wrappedTailer.address();
    }

    @Override
    public void free() {
        wrappedTailer.free();
    }

    @Override
    public void resetLockInt(long offset) {
        wrappedTailer.resetLockInt(offset);
    }

    @Override
    public int threadIdForLockInt(long offset) {
        return wrappedTailer.threadIdForLockInt(offset);
    }

    @Override
    public void resetLockLong(long offset) {
        wrappedTailer.resetLockLong(offset);
    }

    @Override
    public long threadIdForLockLong(long offset) {
        return wrappedTailer.threadIdForLockLong(offset);
    }

    @Override
    public void reserve() {
        wrappedTailer.reserve();
    }

    @Override
    public void release() {
        wrappedTailer.release();
    }

    @Override
    public int refCount() {
        return wrappedTailer.refCount();
    }

    @Override
    public void toString(Appendable sb, long start, long position, long end) {
        wrappedTailer.toString(sb, start, position, end);
    }

    @Override
    public void alignPositionAddr(int alignment) {
        wrappedTailer.alignPositionAddr(alignment);
    }

    @Override
    public void asString(Appendable appendable) {
        wrappedTailer.asString(appendable);
    }

    @Override
    public CharSequence asString() {
        return wrappedTailer.asString();
    }

    @Override
    public void selfTerminating(boolean selfTerminate) {
        wrappedTailer.selfTerminating(selfTerminate);
    }

    @Override
    public boolean selfTerminating() {
        return wrappedTailer.selfTerminating();
    }

    @Override
    public int readUnsignedByteOrThrow() throws BufferUnderflowException {
        return wrappedTailer.readUnsignedByteOrThrow();
    }

    @Override
    public String toDebugString() {
        return wrappedTailer.toDebugString();
    }

    @Override
    public boolean compareAndSwapDouble(long offset, double expected, double x) {
        return wrappedTailer.compareAndSwapDouble(offset, expected, x);
    }

    @Override
    public File file() {
        return wrappedTailer.file();
    }

    @Override
    public boolean tryRWReadLock(long offset, long timeOutNS) throws IllegalStateException {
        return wrappedTailer.tryRWReadLock(offset, timeOutNS);
    }

    @Override
    public boolean tryRWWriteLock(long offset, long timeOutNS) throws IllegalStateException {
        return wrappedTailer.tryRWWriteLock(offset, timeOutNS);
    }

    @Override
    public void unlockRWReadLock(long offset) throws IllegalStateException {
        wrappedTailer.unlockRWReadLock(offset);
    }

    @Override
    public void unlockRWWriteLock(long offset) throws IllegalStateException {
        wrappedTailer.unlockRWWriteLock(offset);
    }

    @Override
    public void readFully(long offset, @org.jetbrains.annotations.NotNull byte[] bytes, int off, int len) {
        wrappedTailer.readFully(offset, bytes, off, len);
    }

    @Override
    public void write(long offset, byte[] bytes, int off, int len) {
        wrappedTailer.write(offset, bytes, off, len);
    }
}
