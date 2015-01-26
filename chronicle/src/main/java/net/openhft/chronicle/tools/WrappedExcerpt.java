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

import net.openhft.chronicle.*;
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

/**
 * @author peter.lawrey
 */
public class WrappedExcerpt implements ExcerptTailer, ExcerptAppender, Excerpt, MappingProvider<WrappedExcerpt> {
    protected ExcerptTailer wrappedTailer;
    protected ExcerptAppender wrappedAppender;
    protected ExcerptCommon wrappedCommon;
    protected Excerpt wrappedExcerpt;
    private MappingFunction withMapping;

    public WrappedExcerpt(ExcerptCommon excerptCommon) {
        setExcerpt(excerptCommon);
    }

    public WrappedExcerpt withMapping(MappingFunction mapping) {
        this.withMapping = mapping;
        return this;
    }

    public MappingFunction withMapping() {
        return this.withMapping;
    }

    protected void setExcerpt(ExcerptCommon excerptCommon) {
        wrappedTailer   = excerptCommon instanceof ExcerptTailer ? (ExcerptTailer) excerptCommon : null;
        wrappedAppender = excerptCommon instanceof ExcerptAppender ? (ExcerptAppender) excerptCommon : null;
        wrappedExcerpt  = excerptCommon instanceof Excerpt ? (Excerpt) excerptCommon : null;
        wrappedCommon   = excerptCommon;
    }

    @Override
    public <E extends Enum<E>> E parseEnum(@NotNull Class<E> eClass, @NotNull StopCharTester tester) {
        return wrappedCommon.parseEnum(eClass, tester);
    }

    @Override
    public WrappedExcerpt clear() {
        wrappedCommon.clear();
        return this;
    }

    @Override
    public void readObject(Object object, int start, int end) {
        wrappedCommon.readObject(object, start, end);
    }

    @Override
    public void writeObject(Object object, int start, int end) {
        wrappedCommon.writeObject(object, start, end);
    }

    @Override
    public Chronicle chronicle() {
        return wrappedCommon.chronicle();
    }

    @Override
    public long size() {
        return wrappedCommon.size();
    }

    @Override
    public boolean nextIndex() {
        return wrappedTailer.nextIndex();
    }

    @Override
    public boolean index(long index) throws IndexOutOfBoundsException {
        return wrappedExcerpt == null ? wrappedTailer.index(index) : wrappedExcerpt.index(index);
    }

    @Override
    public void startExcerpt() {
        wrappedAppender.startExcerpt();
    }

    @Override
    public void startExcerpt(long capacity) {
        wrappedAppender.startExcerpt(capacity);
    }

    @Override
    public void addPaddedEntry() {
        wrappedAppender.addPaddedEntry();
    }

    @Override
    public boolean nextSynchronous() {
        return wrappedAppender.nextSynchronous();
    }

    @Override
    public void nextSynchronous(boolean nextSynchronous) {
        wrappedAppender.nextSynchronous();
    }

    @Override
    public void finish() {
        wrappedCommon.finish();
    }

    @Override
    public long index() {
        return wrappedCommon.index();
    }

    @Override
    public long position() {
        return wrappedCommon.position();
    }

    @Override
    public Boolean parseBoolean(@NotNull StopCharTester tester) {
        return wrappedCommon.parseBoolean(tester);
    }

    @Override
    public long capacity() {
        return wrappedCommon.capacity();
    }

    @Override
    public long remaining() {
        return wrappedCommon.remaining();
    }

    @Override
    public void readFully(@NotNull byte[] bytes) {
        wrappedCommon.readFully(bytes);
    }

    @Override
    public int skipBytes(int n) {
        return wrappedCommon.skipBytes(n);
    }

    @Override
    public void readFully(@NotNull byte[] b, int off, int len) {
        wrappedCommon.readFully(b, off, len);
    }

    @Override
    public boolean readBoolean() {
        return wrappedCommon.readBoolean();
    }

    @Override
    public boolean readBoolean(long offset) {
        return wrappedCommon.readBoolean(offset);
    }

    @Override
    public int readUnsignedByte() {
        return wrappedCommon.readUnsignedByte();
    }

    @Override
    public int readUnsignedByte(long offset) {
        return wrappedCommon.readUnsignedByte(offset);
    }

    @Override
    public int readUnsignedShort() {
        return wrappedCommon.readUnsignedShort();
    }

    @Override
    public int readUnsignedShort(long offset) {
        return wrappedCommon.readUnsignedShort(offset);
    }

    @Override
    public String readLine() {
        return wrappedCommon.readLine();
    }

    @NotNull
    @Override
    public String readUTF() {
        return wrappedCommon.readUTF();
    }

    @Nullable
    @Override
    public String readUTFΔ() {
        return wrappedCommon.readUTFΔ();
    }

    @Nullable
    @Override
    public String readUTFΔ(long offset) throws IllegalStateException {
        return wrappedCommon.readUTFΔ(offset);
    }

    @Override
    public boolean readUTFΔ(@NotNull StringBuilder stringBuilder) {
        return wrappedCommon.readUTFΔ(stringBuilder);
    }

    @NotNull
    @Override
    public String parseUTF(@NotNull StopCharTester tester) {
        return wrappedCommon.parseUTF(tester);
    }

    @Override
    public void parseUTF(@NotNull StringBuilder builder, @NotNull StopCharTester tester) {
        wrappedCommon.parseUTF(builder, tester);
    }

    @Override
    public short readCompactShort() {
        return wrappedCommon.readCompactShort();
    }

    @Override
    public int readCompactUnsignedShort() {
        return wrappedCommon.readCompactUnsignedShort();
    }

    @Override
    public int readInt24() {
        return wrappedCommon.readInt24();
    }

    @Override
    public int readInt24(long offset) {
        return wrappedCommon.readInt24(offset);
    }

    @Override
    public long readUnsignedInt() {
        return wrappedCommon.readUnsignedInt();
    }

    @Override
    public long readUnsignedInt(long offset) {
        return wrappedCommon.readUnsignedInt(offset);
    }

    @Override
    public int readCompactInt() {
        return wrappedCommon.readCompactInt();
    }

    @Override
    public long readCompactUnsignedInt() {
        return wrappedCommon.readCompactUnsignedInt();
    }

    @Override
    public long readInt48() {
        return wrappedCommon.readInt48();
    }

    @Override
    public long readInt48(long offset) {
        return wrappedCommon.readInt48(offset);
    }

    @Override
    public long readCompactLong() {
        return wrappedCommon.readCompactLong();
    }

    @Override
    public long readStopBit() {
        return wrappedCommon.readStopBit();
    }

    @Override
    public double readCompactDouble() {
        return wrappedCommon.readCompactDouble();
    }

    @Override
    public void read(@NotNull ByteBuffer bb) {
        wrappedCommon.read(bb);
    }

    @Override
    public void write(byte[] bytes) {
        wrappedCommon.write(bytes);
    }

    @Override
    public void write(char[] bytes) {
        wrappedCommon.write(bytes);
    }

    @Override
    public void write(@NotNull char[] data, int off, int len) {
        wrappedCommon.write(data, off, len);
    }

    @Override
    public ByteBuffer sliceAsByteBuffer(@Nullable ByteBuffer toReuse) {
        return wrappedCommon.sliceAsByteBuffer(toReuse);
    }

    @Override
    public void readFully(@NotNull char[] data) {
        wrappedCommon.readFully(data);
    }

    @Override
    public void readFully(@NotNull char[] data, int off, int len) {
        wrappedCommon.readFully(data, off, len);
    }

    @Override
    public void writeChars(@NotNull CharSequence cs) {
        wrappedCommon.writeChars(cs);
    }

    @Override
    public void writeBoolean(boolean v) {
        wrappedCommon.writeBoolean(v);
    }

    @Override
    public void writeBoolean(long offset, boolean v) {
        wrappedCommon.writeBoolean(offset, v);
    }

    @Override
    public void writeBytes(@NotNull String s) {
        wrappedCommon.writeBytes(s);
    }

    @Override
    public void writeChars(@NotNull String s) {
        wrappedCommon.writeChars(s);
    }

    @Override
    public void writeUTF(@NotNull String s) {
        wrappedCommon.writeUTF(s);
    }

    @Override
    public void writeUTFΔ(CharSequence str) {
        wrappedCommon.writeUTFΔ(str);
    }

    @Override
    public void writeUTFΔ(long offset, int maxSize, @Nullable CharSequence s) throws IllegalStateException {
        wrappedCommon.writeUTFΔ(offset, maxSize, s);
    }

    @Override
    public void writeByte(int v) {
        wrappedCommon.writeByte(v);
    }

    @Override
    public void writeUnsignedByte(int v) {
        wrappedCommon.writeUnsignedByte(v);
    }

    @Override
    public void writeUnsignedByte(long offset, int v) {
        wrappedCommon.writeUnsignedByte(offset, v);
    }

    @Override
    public void write(long offset, byte[] bytes) {
        wrappedCommon.write(offset, bytes);
    }

    @Override
    public void write(byte[] bytes, int off, int len) {
        wrappedCommon.write(bytes, off, len);
    }

    @Override
    public void writeUnsignedShort(int v) {
        wrappedCommon.writeUnsignedShort(v);
    }

    @Override
    public void writeUnsignedShort(long offset, int v) {
        wrappedCommon.writeUnsignedShort(offset, v);
    }

    @Override
    public void writeCompactShort(int v) {
        wrappedCommon.writeCompactShort(v);
    }

    @Override
    public void writeCompactUnsignedShort(int v) {
        wrappedCommon.writeCompactUnsignedShort(v);
    }

    @Override
    public void writeInt24(int v) {
        wrappedCommon.writeInt24(v);
    }

    @Override
    public void writeInt24(long offset, int v) {
        wrappedCommon.writeInt24(offset, v);
    }

    @Override
    public void writeUnsignedInt(long v) {
        wrappedCommon.writeUnsignedInt(v);
    }

    @Override
    public void writeUnsignedInt(long offset, long v) {
        wrappedCommon.writeUnsignedInt(offset, v);
    }

    @Override
    public void writeCompactInt(int v) {
        wrappedCommon.writeCompactInt(v);
    }

    @Override
    public void writeCompactUnsignedInt(long v) {
        wrappedCommon.writeCompactUnsignedInt(v);
    }

    @Override
    public void writeInt48(long v) {
        wrappedCommon.writeInt48(v);
    }

    @Override
    public void writeInt48(long offset, long v) {
        wrappedCommon.writeInt48(offset, v);
    }

    @Override
    public void writeCompactLong(long v) {
        wrappedCommon.writeCompactLong(v);
    }

    @Override
    public void writeCompactDouble(double v) {
        wrappedCommon.writeCompactDouble(v);
    }

    @Override
    public void write(@NotNull ByteBuffer bb) {
        wrappedCommon.write(bb);
    }

    @NotNull
    @Override
    public ByteStringAppender append(@NotNull CharSequence s) {
        wrappedCommon.append(s);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender append(@NotNull CharSequence s, int start, int end) {
        wrappedCommon.append(s, start, end);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender append(@Nullable Enum value) {
        wrappedCommon.append(value);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender append(boolean b) {
        wrappedCommon.append(b);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender append(char c) {
        wrappedCommon.append(c);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender append(int num) {
        wrappedCommon.append(num);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender append(long num) {
        wrappedCommon.append(num);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender append(double d) {
        wrappedCommon.append(d);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender append(double d, int precision) {
        wrappedCommon.append(d, precision);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender append(@NotNull MutableDecimal md) {
        wrappedCommon.append(md);
        return this;
    }

    @Override
    public double parseDouble() {
        return wrappedCommon.parseDouble();
    }

    @Override
    public long parseLong() {
        return wrappedCommon.parseLong();
    }

    @NotNull
    @Override
    public InputStream inputStream() {
        return wrappedCommon.inputStream();
    }

    @NotNull
    @Override
    public OutputStream outputStream() {
        return wrappedCommon.outputStream();
    }

    @Override
    public ObjectSerializer objectSerializer() {
        return wrappedCommon.objectSerializer();
    }

    @Override
    public <E> void writeEnum(E o) {
        wrappedCommon.writeEnum(o);
    }

    @Override
    public <E> E readEnum(@NotNull Class<E> aClass) {
        return wrappedCommon.readEnum(aClass);
    }

    @Override
    public <K, V> void writeMap(@NotNull Map<K, V> map) {
        wrappedCommon.writeMap(map);
    }

    @Override
    public <K, V> Map<K, V> readMap(@NotNull Map<K, V> map, @NotNull Class<K> kClass, @NotNull Class<V> vClass) {
        return wrappedCommon.readMap(map, kClass, vClass);
    }

    @Override
    public byte readByte() {
        return wrappedCommon.readByte();
    }

    @Override
    public byte readByte(long offset) {
        return wrappedCommon.readByte(offset);
    }

    @Override
    public short readShort() {
        return wrappedCommon.readShort();
    }

    @Override
    public short readShort(long offset) {
        return wrappedCommon.readShort(offset);
    }

    @Override
    public char readChar() {
        return wrappedCommon.readChar();
    }

    @Override
    public char readChar(long offset) {
        return wrappedCommon.readChar(offset);
    }

    @Override
    public int readInt() {
        return wrappedCommon.readInt();
    }

    @Override
    public int readInt(long offset) {
        return wrappedCommon.readInt(offset);
    }

    @Override
    public long readLong() {
        return wrappedCommon.readLong();
    }

    @Override
    public long readLong(long offset) {
        return wrappedCommon.readLong(offset);
    }

    @Override
    public float readFloat() {
        return wrappedCommon.readFloat();
    }

    @Override
    public float readFloat(long offset) {
        return wrappedCommon.readFloat(offset);
    }

    @Override
    public double readDouble() {
        return wrappedCommon.readDouble();
    }

    @Override
    public double readDouble(long offset) {
        return wrappedCommon.readDouble(offset);
    }

    @Override
    public void write(int b) {
        wrappedCommon.write(b);
    }

    @Override
    public void writeByte(long offset, int b) {
        wrappedCommon.writeByte(offset, b);
    }

    @Override
    public void writeShort(int v) {
        wrappedCommon.writeShort(v);
    }

    @Override
    public void writeShort(long offset, int v) {
        wrappedCommon.writeShort(offset, v);
    }

    @Override
    public void writeChar(int v) {
        wrappedCommon.writeChar(v);
    }

    @Override
    public void writeChar(long offset, int v) {
        wrappedCommon.writeChar(offset, v);
    }

    @Override
    public void writeInt(int v) {
        wrappedCommon.writeInt(v);
    }

    @Override
    public void writeInt(long offset, int v) {
        wrappedCommon.writeInt(offset, v);
    }

    @Override
    public void writeLong(long v) {
        wrappedCommon.writeLong(v);
    }

    @Override
    public void writeLong(long offset, long v) {
        wrappedCommon.writeLong(offset, v);
    }

    @Override
    public void writeStopBit(long n) {
        wrappedCommon.writeStopBit(n);
    }

    @Override
    public void writeFloat(float v) {
        wrappedCommon.writeFloat(v);
    }

    @Override
    public void writeFloat(long offset, float v) {
        wrappedCommon.writeFloat(offset, v);
    }

    @Override
    public void writeDouble(double v) {
        wrappedCommon.writeDouble(v);
    }

    @Override
    public void writeDouble(long offset, double v) {
        wrappedCommon.writeDouble(offset, v);
    }

    @Nullable
    @Override
    public Object readObject() {
        return wrappedCommon.readObject();
    }

    @Override
    public <T> T readObject(Class<T> tClass) throws IllegalStateException {
        return wrappedCommon.readObject(tClass);
    }

    @Override
    public int read() {
        return wrappedCommon.read();
    }

    @Override
    public int read(@NotNull byte[] bytes) {
        return wrappedCommon.read(bytes);
    }

    @Override
    public int read(@NotNull byte[] bytes, int off, int len) {
        return wrappedCommon.read(bytes, off, len);
    }

    @Override
    public long skip(long n) {
        return wrappedCommon.skip(n);
    }

    @Override
    public int available() {
        return wrappedCommon.available();
    }

    @Override
    public void close() {
        try {
            wrappedCommon.close();
        } catch (Exception ignored) {
        }
    }

    @Override
    public void writeObject(Object obj) {
        wrappedCommon.writeObject(obj);
    }

    @Override
    public void flush() {
        wrappedCommon.flush();
    }

    @Override
    public <E> void writeList(@NotNull Collection<E> list) {
        wrappedCommon.writeList(list);
    }

    @Override
    public <E> void readList(@NotNull Collection<E> list, @NotNull Class<E> eClass) {
        wrappedCommon.readList(list, eClass);
    }

    @Override
    public long lastWrittenIndex() {
        return wrappedAppender.lastWrittenIndex();
    }

    @Override
    public boolean stepBackAndSkipTo(@NotNull StopCharTester tester) {
        return wrappedCommon.stepBackAndSkipTo(tester);
    }

    @Override
    public boolean skipTo(@NotNull StopCharTester tester) {
        return wrappedCommon.skipTo(tester);
    }

    @NotNull
    @Override
    public MutableDecimal parseDecimal(@NotNull MutableDecimal decimal) {
        return wrappedCommon.parseDecimal(decimal);
    }

    @NotNull
    @Override
    public Excerpt toStart() {
        if (wrappedTailer == null) {
            wrappedExcerpt.toStart();
        } else {
            wrappedTailer.toStart();
        }

        return this;
    }

    @NotNull
    @Override
    public Excerpt toEnd() {
        wrappedTailer.toEnd();
        return this;
    }

    @Override
    public boolean isFinished() {
        return wrappedCommon.isFinished();
    }

    @Override
    public boolean wasPadding() {
        return wrappedCommon.wasPadding();
    }

    @NotNull
    @Override
    public ByteStringAppender appendTimeMillis(long timeInMS) {
        wrappedCommon.appendTimeMillis(timeInMS);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender appendDateMillis(long timeInMS) {
        wrappedCommon.appendDateMillis(timeInMS);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender appendDateTimeMillis(long timeInMS) {
        wrappedCommon.appendDateTimeMillis(timeInMS);
        return this;
    }

    @NotNull
    @Override
    public <E> ByteStringAppender append(@NotNull Iterable<E> list, @NotNull CharSequence seperator) {
        wrappedCommon.append(list, seperator);
        return this;
    }

    @Override
    public int readVolatileInt() {
        return wrappedCommon.readVolatileInt();
    }

    @Override
    public int readVolatileInt(long offset) {
        return wrappedCommon.readVolatileInt(offset);
    }

    @Override
    public long readVolatileLong() {
        return wrappedCommon.readVolatileLong();
    }

    @Override
    public long readVolatileLong(long offset) {
        return wrappedCommon.readVolatileLong(offset);
    }

    @Override
    public void writeOrderedInt(int v) {
        wrappedCommon.writeOrderedInt(v);
    }

    @Override
    public void writeOrderedInt(long offset, int v) {
        wrappedCommon.writeOrderedInt(offset, v);
    }

    @Override
    public boolean compareAndSwapInt(long offset, int expected, int x) {
        return wrappedCommon.compareAndSwapInt(offset, expected, x);
    }

    @Override
    public int getAndAdd(long offset, int delta) {
        return wrappedCommon.getAndAdd(offset, delta);
    }

    @Override
    public int addAndGetInt(long offset, int delta) {
        return wrappedCommon.addAndGetInt(offset, delta);
    }

    @Override
    public void writeOrderedLong(long v) {
        wrappedCommon.writeOrderedLong(v);
    }

    @Override
    public void writeOrderedLong(long offset, long v) {
        wrappedCommon.writeOrderedLong(offset, v);
    }

    @Override
    public boolean compareAndSwapLong(long offset, long expected, long x) {
        return wrappedCommon.compareAndSwapLong(offset, expected, x);
    }

    @Override
    public WrappedExcerpt position(long position) {
        wrappedCommon.position(position);
        return this;
    }

    @NotNull
    @Override
    public ByteOrder byteOrder() {
        return wrappedCommon.byteOrder();
    }

    @Override
    public void checkEndOfBuffer() throws IndexOutOfBoundsException {
        wrappedCommon.checkEndOfBuffer();
    }

    @Override
    public boolean tryLockInt(long offset) {
        return wrappedCommon.tryLockInt(offset);
    }

    @Override
    public boolean tryLockNanosInt(long offset, long nanos) {
        return wrappedCommon.tryLockNanosInt(offset, nanos);
    }

    @Override
    public void busyLockInt(long offset) throws InterruptedException, IllegalStateException {
        wrappedCommon.busyLockInt(offset);
    }

    @Override
    public void unlockInt(long offset) throws IllegalStateException {
        wrappedCommon.unlockInt(offset);
    }

    @Override
    public boolean tryLockLong(long offset) {
        return wrappedCommon.tryLockLong(offset);
    }

    @Override
    public boolean tryLockNanosLong(long offset, long nanos) {
        return wrappedCommon.tryLockNanosLong(offset, nanos);
    }

    @Override
    public void busyLockLong(long offset) throws InterruptedException, IllegalStateException {
        wrappedCommon.busyLockLong(offset);
    }

    @Override
    public void unlockLong(long offset) throws IllegalStateException {
        wrappedCommon.unlockLong(offset);
    }

    @Override
    public float readVolatileFloat(long offset) {
        return wrappedCommon.readVolatileFloat(offset);
    }

    @Override
    public double readVolatileDouble(long offset) {
        return wrappedCommon.readVolatileDouble(offset);
    }

    @Override
    public void writeOrderedFloat(long offset, float v) {
        wrappedCommon.writeOrderedFloat(offset, v);
    }

    @Override
    public void writeOrderedDouble(long offset, double v) {
        wrappedCommon.writeOrderedDouble(offset, v);
    }

    @Override
    public byte addByte(long offset, byte b) {
        return wrappedCommon.addByte(offset, b);
    }

    @Override
    public int addUnsignedByte(long offset, int i) {
        return wrappedCommon.addUnsignedByte(offset, i);
    }

    @Override
    public short addShort(long offset, short s) {
        return wrappedCommon.addShort(offset, s);
    }

    @Override
    public int addUnsignedShort(long offset, int i) {
        return wrappedCommon.addUnsignedShort(offset, i);
    }

    @Override
    public int addInt(long offset, int i) {
        return wrappedCommon.addInt(offset, i);
    }

    @Override
    public long addUnsignedInt(long offset, long i) {
        return wrappedCommon.addUnsignedInt(offset, i);
    }

    @Override
    public long addLong(long offset, long i) {
        return wrappedCommon.addLong(offset, i);
    }

    @Override
    public float addFloat(long offset, float f) {
        return wrappedCommon.addFloat(offset, f);
    }

    @Override
    public double addDouble(long offset, double d) {
        return wrappedCommon.addDouble(offset, d);
    }

    @Override
    public int addAtomicInt(long offset, int i) {
        return wrappedCommon.addAtomicInt(offset, i);
    }

    @Override
    public long addAtomicLong(long offset, long l) {
        return wrappedCommon.addAtomicLong(offset, l);
    }

    @Override
    public float addAtomicFloat(long offset, float f) {
        return wrappedCommon.addAtomicFloat(offset, f);
    }

    @Override
    public double addAtomicDouble(long offset, double d) {
        return wrappedCommon.addAtomicDouble(offset, d);
    }

    @Override
    public long findMatch(@NotNull ExcerptComparator comparator) {
        return wrappedExcerpt.findMatch(comparator);
    }

    @Override
    public void findRange(@NotNull long[] startEnd, @NotNull ExcerptComparator comparator) {
        wrappedExcerpt.findRange(startEnd, comparator);
    }

    @NotNull
    @Override
    public ByteStringAppender append(long l, int base) {
        wrappedCommon.append(l, base);
        return this;
    }

    @Override
    public long parseLong(int base) {
        return wrappedCommon.parseLong(base);
    }

    @Override
    public void write(RandomDataInput bytes, long position, long length) {
        wrappedCommon.write(bytes, position, length);
    }

    @Override
    public void readMarshallable(@NotNull Bytes in) throws IllegalStateException {
        wrappedCommon.readMarshallable(in);
    }

    @Override
    public void writeMarshallable(@NotNull Bytes out) {
        wrappedCommon.writeMarshallable(out);
    }

    @Override
    public int length() {
        return wrappedCommon.length();
    }

    @Override
    public char charAt(int index) {
        return wrappedCommon.charAt(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return wrappedCommon.subSequence(start, end);
    }

    @Override
    public Bytes flip() {
        return wrappedCommon.flip();
    }

    @NotNull
    @Override
    public <T> T readInstance(@NotNull Class<T> objClass, T obj) {
        return wrappedCommon.readInstance(objClass, obj);
    }

    @Override
    public boolean startsWith(RandomDataInput keyBytes) {
        return wrappedCommon.startsWith(keyBytes);
    }

    @Override
    public void write(RandomDataInput bytes) {
        wrappedCommon.write(bytes);
    }

    @Override
    public <OBJ> void writeInstance(@NotNull Class<OBJ> objClass, @NotNull OBJ obj) {
        wrappedCommon.writeInstance(objClass, obj);
    }

    @Override
    public Bytes zeroOut() {
        wrappedCommon.zeroOut();
        return this;
    }

    @Override
    public Bytes zeroOut(long start, long end, boolean ifNotZero) {
        wrappedCommon.zeroOut(start, end, ifNotZero);
        return this;
    }

    @Override
    public Bytes zeroOut(long start, long end) {
        wrappedCommon.zeroOut(start, end);
        return this;
    }

    @Override
    public long limit() {
        return wrappedCommon.limit();
    }

    @Override
    public Bytes limit(long limit) {
        wrappedCommon.limit(limit);
        return this;
    }

    @Override
    public Bytes load() {
        wrappedCommon.load();
        return this;
    }

    @Override
    public Bytes slice() {
        return wrappedCommon.slice();
    }

    @Override
    public Bytes slice(long offset, long length) {
        return wrappedCommon.slice(offset, length);
    }

    @Override
    public Bytes bytes() {
        return wrappedCommon.bytes();
    }

    @Override
    public Bytes bytes(long offset, long length) {
        return wrappedCommon.bytes(offset, length);
    }

    @Override
    public long address() {
        return wrappedCommon.address();
    }

    @Override
    public void free() {
        wrappedCommon.free();
    }

    @Override
    public void resetLockInt(long offset) {
        wrappedCommon.resetLockInt(offset);
    }

    @Override
    public int threadIdForLockInt(long offset) {
        return wrappedCommon.threadIdForLockInt(offset);
    }

    @Override
    public void resetLockLong(long offset) {
        wrappedCommon.resetLockLong(offset);
    }

    @Override
    public long threadIdForLockLong(long offset) {
        return wrappedCommon.threadIdForLockLong(offset);
    }

    @Override
    public void reserve() {
        wrappedCommon.reserve();
    }

    @Override
    public void release() {
        wrappedCommon.release();
    }

    @Override
    public int refCount() {
        return wrappedCommon.refCount();
    }

    @Override
    public void toString(Appendable sb, long start, long position, long end) {
        wrappedCommon.toString(sb, start, position, end);
    }

    @Override
    public void alignPositionAddr(int alignment) {
        wrappedCommon.alignPositionAddr(alignment);
    }

    @Override
    public void asString(Appendable appendable) {
        wrappedCommon.asString(appendable);
    }

    @Override
    public CharSequence asString() {
        return wrappedCommon.asString();
    }

    @Override
    public void selfTerminating(boolean selfTerminate) {
        wrappedCommon.selfTerminating(selfTerminate);
    }

    @Override
    public boolean selfTerminating() {
        return wrappedCommon.selfTerminating();
    }

    @Override
    public int readUnsignedByteOrThrow() throws BufferUnderflowException {
        return wrappedCommon.readUnsignedByteOrThrow();
    }

    @Override
    public String toDebugString() {
        return wrappedCommon.toDebugString();
    }

    @Override
    public String toDebugString(long limit) {
        return wrappedCommon.toDebugString(limit);
    }

    @Override
    public boolean compareAndSwapDouble(long offset, double expected, double x) {
        return wrappedCommon.compareAndSwapDouble(offset, expected, x);
    }

    @Override
    public File file() {
        return wrappedCommon.file();
    }

    @Override
    public boolean tryRWReadLock(long offset, long timeOutNS) throws IllegalStateException {
        return wrappedCommon.tryRWReadLock(offset, timeOutNS);
    }

    @Override
    public boolean tryRWWriteLock(long offset, long timeOutNS) throws IllegalStateException {
        return wrappedCommon.tryRWWriteLock(offset, timeOutNS);
    }

    @Override
    public void unlockRWReadLock(long offset) throws IllegalStateException {
        wrappedCommon.unlockRWReadLock(offset);
    }

    @Override
    public void unlockRWWriteLock(long offset) throws IllegalStateException {
        wrappedCommon.unlockRWWriteLock(offset);
    }

    @Override
    public void readFully(long offset, @org.jetbrains.annotations.NotNull byte[] bytes, int off, int len) {
        wrappedCommon.readFully(offset, bytes, off, len);
    }

    @Override
    public void write(long offset, byte[] bytes, int off, int len) {
        wrappedCommon.write(offset, bytes, off, len);
    }
}
