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
public class WrappedExcerpt implements ExcerptTailer, ExcerptAppender, Excerpt {
    protected ExcerptTailer delegatedTailer;
    protected ExcerptAppender delegatedAppender;
    protected ExcerptCommon delegatedCommon;
    protected Excerpt delegatedExcerpt;

    public WrappedExcerpt(ExcerptCommon excerptCommon) {
        setExcerpt(excerptCommon);
    }

    protected void setExcerpt(ExcerptCommon excerptCommon) {
        delegatedTailer = excerptCommon instanceof ExcerptTailer ? (ExcerptTailer) excerptCommon : null;
        delegatedAppender = excerptCommon instanceof ExcerptAppender ? (ExcerptAppender) excerptCommon : null;
        delegatedExcerpt = excerptCommon instanceof Excerpt ? (Excerpt) excerptCommon : null;
        delegatedCommon = excerptCommon;
    }

    @Override
    public <E extends Enum<E>> E parseEnum(@NotNull Class<E> eClass, @NotNull StopCharTester tester) {
        return delegatedCommon.parseEnum(eClass, tester);
    }

    @Override
    public WrappedExcerpt clear() {
        delegatedCommon.clear();
        return this;
    }

    @Override
    public void readObject(Object object, int start, int end) {
        delegatedCommon.readObject(object, start, end);
    }

    @Override
    public void writeObject(Object object, int start, int end) {
        delegatedCommon.writeObject(object, start, end);
    }

    @Override
    public Chronicle chronicle() {
        return delegatedCommon.chronicle();
    }

    @Override
    public long size() {
        return delegatedCommon.size();
    }

    @Override
    public boolean nextIndex() {
        return delegatedTailer.nextIndex();
    }

    @Override
    public boolean index(long index) throws IndexOutOfBoundsException {
        return delegatedExcerpt == null ? delegatedTailer.index(index) : delegatedExcerpt.index(index);
    }

    @Override
    public void startExcerpt() {
        delegatedAppender.startExcerpt();
    }

    @Override
    public void startExcerpt(long capacity) {
        delegatedAppender.startExcerpt(capacity);
    }

    @Override
    public void addPaddedEntry() {
        delegatedAppender.addPaddedEntry();
    }

    @Override
    public boolean nextSynchronous() {
        return delegatedAppender.nextSynchronous();
    }

    @Override
    public void nextSynchronous(boolean nextSynchronous) {
        delegatedAppender.nextSynchronous();
    }

    @Override
    public void finish() {
        delegatedCommon.finish();
    }

    @Override
    public long index() {
        return delegatedCommon.index();
    }

    @Override
    public long position() {
        return delegatedCommon.position();
    }

    @Override
    public Boolean parseBoolean(@NotNull StopCharTester tester) {
        return delegatedCommon.parseBoolean(tester);
    }

    @Override
    public long capacity() {
        return delegatedCommon.capacity();
    }

    @Override
    public long remaining() {
        return delegatedCommon.remaining();
    }

    @Override
    public void readFully(@NotNull byte[] bytes) {
        delegatedCommon.readFully(bytes);
    }

    @Override
    public int skipBytes(int n) {
        return delegatedCommon.skipBytes(n);
    }

    @Override
    public void readFully(@NotNull byte[] b, int off, int len) {
        delegatedCommon.readFully(b, off, len);
    }

    @Override
    public boolean readBoolean() {
        return delegatedCommon.readBoolean();
    }

    @Override
    public boolean readBoolean(long offset) {
        return delegatedCommon.readBoolean(offset);
    }

    @Override
    public int readUnsignedByte() {
        return delegatedCommon.readUnsignedByte();
    }

    @Override
    public int readUnsignedByte(long offset) {
        return delegatedCommon.readUnsignedByte(offset);
    }

    @Override
    public int readUnsignedShort() {
        return delegatedCommon.readUnsignedShort();
    }

    @Override
    public int readUnsignedShort(long offset) {
        return delegatedCommon.readUnsignedShort(offset);
    }

    @Override
    public String readLine() {
        return delegatedCommon.readLine();
    }

    @NotNull
    @Override
    public String readUTF() {
        return delegatedCommon.readUTF();
    }

    @Nullable
    @Override
    public String readUTFΔ() {
        return delegatedCommon.readUTFΔ();
    }

    @Nullable
    @Override
    public String readUTFΔ(long offset) throws IllegalStateException {
        return delegatedCommon.readUTFΔ(offset);
    }

    @Override
    public boolean readUTFΔ(@NotNull StringBuilder stringBuilder) {
        return delegatedCommon.readUTFΔ(stringBuilder);
    }

    @NotNull
    @Override
    public String parseUTF(@NotNull StopCharTester tester) {
        return delegatedCommon.parseUTF(tester);
    }

    @Override
    public void parseUTF(@NotNull StringBuilder builder, @NotNull StopCharTester tester) {
        delegatedCommon.parseUTF(builder, tester);
    }

    @Override
    public short readCompactShort() {
        return delegatedCommon.readCompactShort();
    }

    @Override
    public int readCompactUnsignedShort() {
        return delegatedCommon.readCompactUnsignedShort();
    }

    @Override
    public int readInt24() {
        return delegatedCommon.readInt24();
    }

    @Override
    public int readInt24(long offset) {
        return delegatedCommon.readInt24(offset);
    }

    @Override
    public long readUnsignedInt() {
        return delegatedCommon.readUnsignedInt();
    }

    @Override
    public long readUnsignedInt(long offset) {
        return delegatedCommon.readUnsignedInt(offset);
    }

    @Override
    public int readCompactInt() {
        return delegatedCommon.readCompactInt();
    }

    @Override
    public long readCompactUnsignedInt() {
        return delegatedCommon.readCompactUnsignedInt();
    }

    @Override
    public long readInt48() {
        return delegatedCommon.readInt48();
    }

    @Override
    public long readInt48(long offset) {
        return delegatedCommon.readInt48(offset);
    }

    @Override
    public long readCompactLong() {
        return delegatedCommon.readCompactLong();
    }

    @Override
    public long readStopBit() {
        return delegatedCommon.readStopBit();
    }

    @Override
    public double readCompactDouble() {
        return delegatedCommon.readCompactDouble();
    }

    @Override
    public void read(@NotNull ByteBuffer bb) {
        delegatedCommon.read(bb);
    }

    @Override
    public void write(byte[] bytes) {
        delegatedCommon.write(bytes);
    }

    @Override
    public void writeBoolean(boolean v) {
        delegatedCommon.writeBoolean(v);
    }

    @Override
    public void writeBoolean(long offset, boolean v) {
        delegatedCommon.writeBoolean(offset, v);
    }

    @Override
    public void writeBytes(@NotNull String s) {
        delegatedCommon.writeBytes(s);
    }

    @Override
    public void writeChars(@NotNull String s) {
        delegatedCommon.writeChars(s);
    }

    @Override
    public void writeUTF(@NotNull String s) {
        delegatedCommon.writeUTF(s);
    }

    @Override
    public void writeUTFΔ(CharSequence str) {
        delegatedCommon.writeUTFΔ(str);
    }

    @Override
    public void writeUTFΔ(long offset, int maxSize, @Nullable CharSequence s) throws IllegalStateException {
        delegatedCommon.writeUTFΔ(offset, maxSize, s);
    }

    @Override
    public void writeByte(int v) {
        delegatedCommon.writeByte(v);
    }

    @Override
    public void writeUnsignedByte(int v) {
        delegatedCommon.writeUnsignedByte(v);
    }

    @Override
    public void writeUnsignedByte(long offset, int v) {
        delegatedCommon.writeUnsignedByte(offset, v);
    }

    @Override
    public void write(long offset, byte[] bytes) {
        delegatedCommon.write(offset, bytes);
    }

    @Override
    public void write(byte[] bytes, int off, int len) {
        delegatedCommon.write(bytes, off, len);
    }

    @Override
    public void writeUnsignedShort(int v) {
        delegatedCommon.writeUnsignedShort(v);
    }

    @Override
    public void writeUnsignedShort(long offset, int v) {
        delegatedCommon.writeUnsignedShort(offset, v);
    }

    @Override
    public void writeCompactShort(int v) {
        delegatedCommon.writeCompactShort(v);
    }

    @Override
    public void writeCompactUnsignedShort(int v) {
        delegatedCommon.writeCompactUnsignedShort(v);
    }

    @Override
    public void writeInt24(int v) {
        delegatedCommon.writeInt24(v);
    }

    @Override
    public void writeInt24(long offset, int v) {
        delegatedCommon.writeInt24(offset, v);
    }

    @Override
    public void writeUnsignedInt(long v) {
        delegatedCommon.writeUnsignedInt(v);
    }

    @Override
    public void writeUnsignedInt(long offset, long v) {
        delegatedCommon.writeUnsignedInt(offset, v);
    }

    @Override
    public void writeCompactInt(int v) {
        delegatedCommon.writeCompactInt(v);
    }

    @Override
    public void writeCompactUnsignedInt(long v) {
        delegatedCommon.writeCompactUnsignedInt(v);
    }

    @Override
    public void writeInt48(long v) {
        delegatedCommon.writeInt48(v);
    }

    @Override
    public void writeInt48(long offset, long v) {
        delegatedCommon.writeInt48(offset, v);
    }

    @Override
    public void writeCompactLong(long v) {
        delegatedCommon.writeCompactLong(v);
    }

    @Override
    public void writeCompactDouble(double v) {
        delegatedCommon.writeCompactDouble(v);
    }

    @Override
    public void write(@NotNull ByteBuffer bb) {
        delegatedCommon.write(bb);
    }

    @NotNull
    @Override
    public ByteStringAppender append(@NotNull CharSequence s) {
        delegatedCommon.append(s);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender append(@NotNull CharSequence s, int start, int end) {
        delegatedCommon.append(s, start, end);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender append(@Nullable Enum value) {
        delegatedCommon.append(value);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender append(boolean b) {
        delegatedCommon.append(b);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender append(char c) {
        delegatedCommon.append(c);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender append(int num) {
        delegatedCommon.append(num);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender append(long num) {
        delegatedCommon.append(num);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender append(double d) {
        delegatedCommon.append(d);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender append(double d, int precision) {
        delegatedCommon.append(d, precision);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender append(@NotNull MutableDecimal md) {
        delegatedCommon.append(md);
        return this;
    }

    @Override
    public double parseDouble() {
        return delegatedCommon.parseDouble();
    }

    @Override
    public long parseLong() {
        return delegatedCommon.parseLong();
    }

    @NotNull
    @Override
    public InputStream inputStream() {
        return delegatedCommon.inputStream();
    }

    @NotNull
    @Override
    public OutputStream outputStream() {
        return delegatedCommon.outputStream();
    }

    @Override
    public ObjectSerializer objectSerializer() {
        return delegatedCommon.objectSerializer();
    }

    @Override
    public <E> void writeEnum(E o) {
        delegatedCommon.writeEnum(o);
    }

    @Override
    public <E> E readEnum(@NotNull Class<E> aClass) {
        return delegatedCommon.readEnum(aClass);
    }

    @Override
    public <K, V> void writeMap(@NotNull Map<K, V> map) {
        delegatedCommon.writeMap(map);
    }

    @Override
    public <K, V> Map<K, V> readMap(@NotNull Map<K, V> map, @NotNull Class<K> kClass, @NotNull Class<V> vClass) {
        return delegatedCommon.readMap(map, kClass, vClass);
    }

    @Override
    public byte readByte() {
        return delegatedCommon.readByte();
    }

    @Override
    public byte readByte(long offset) {
        return delegatedCommon.readByte(offset);
    }

    @Override
    public short readShort() {
        return delegatedCommon.readShort();
    }

    @Override
    public short readShort(long offset) {
        return delegatedCommon.readShort(offset);
    }

    @Override
    public char readChar() {
        return delegatedCommon.readChar();
    }

    @Override
    public char readChar(long offset) {
        return delegatedCommon.readChar(offset);
    }

    @Override
    public int readInt() {
        return delegatedCommon.readInt();
    }

    @Override
    public int readInt(long offset) {
        return delegatedCommon.readInt(offset);
    }

    @Override
    public long readLong() {
        return delegatedCommon.readLong();
    }

    @Override
    public long readLong(long offset) {
        return delegatedCommon.readLong(offset);
    }

    @Override
    public float readFloat() {
        return delegatedCommon.readFloat();
    }

    @Override
    public float readFloat(long offset) {
        return delegatedCommon.readFloat(offset);
    }

    @Override
    public double readDouble() {
        return delegatedCommon.readDouble();
    }

    @Override
    public double readDouble(long offset) {
        return delegatedCommon.readDouble(offset);
    }

    @Override
    public void write(int b) {
        delegatedCommon.write(b);
    }

    @Override
    public void writeByte(long offset, int b) {
        delegatedCommon.writeByte(offset, b);
    }

    @Override
    public void writeShort(int v) {
        delegatedCommon.writeShort(v);
    }

    @Override
    public void writeShort(long offset, int v) {
        delegatedCommon.writeShort(offset, v);
    }

    @Override
    public void writeChar(int v) {
        delegatedCommon.writeChar(v);
    }

    @Override
    public void writeChar(long offset, int v) {
        delegatedCommon.writeChar(offset, v);
    }

    @Override
    public void writeInt(int v) {
        delegatedCommon.writeInt(v);
    }

    @Override
    public void writeInt(long offset, int v) {
        delegatedCommon.writeInt(offset, v);
    }

    @Override
    public void writeLong(long v) {
        delegatedCommon.writeLong(v);
    }

    @Override
    public void writeLong(long offset, long v) {
        delegatedCommon.writeLong(offset, v);
    }

    @Override
    public void writeStopBit(long n) {
        delegatedCommon.writeStopBit(n);
    }

    @Override
    public void writeFloat(float v) {
        delegatedCommon.writeFloat(v);
    }

    @Override
    public void writeFloat(long offset, float v) {
        delegatedCommon.writeFloat(offset, v);
    }

    @Override
    public void writeDouble(double v) {
        delegatedCommon.writeDouble(v);
    }

    @Override
    public void writeDouble(long offset, double v) {
        delegatedCommon.writeDouble(offset, v);
    }

    @Nullable
    @Override
    public Object readObject() {
        return delegatedCommon.readObject();
    }

    @Override
    public <T> T readObject(Class<T> tClass) throws IllegalStateException {
        return delegatedCommon.readObject(tClass);
    }

    @Override
    public int read() {
        return delegatedCommon.read();
    }

    @Override
    public int read(@NotNull byte[] bytes) {
        return delegatedCommon.read(bytes);
    }

    @Override
    public int read(@NotNull byte[] bytes, int off, int len) {
        return delegatedCommon.read(bytes, off, len);
    }

    @Override
    public long skip(long n) {
        return delegatedCommon.skip(n);
    }

    @Override
    public int available() {
        return delegatedCommon.available();
    }

    @Override
    public void close() {
        try {
            delegatedCommon.close();
        } catch (Exception ignored) {
        }
    }

    @Override
    public void writeObject(Object obj) {
        delegatedCommon.writeObject(obj);
    }

    @Override
    public void flush() {
        delegatedCommon.flush();
    }

    @Override
    public <E> void writeList(@NotNull Collection<E> list) {
        delegatedCommon.writeList(list);
    }

    @Override
    public <E> void readList(@NotNull Collection<E> list, @NotNull Class<E> eClass) {
        delegatedCommon.readList(list, eClass);
    }

    @Override
    public long lastWrittenIndex() {
        return delegatedCommon.lastWrittenIndex();
    }

    @Override
    public boolean stepBackAndSkipTo(@NotNull StopCharTester tester) {
        return delegatedCommon.stepBackAndSkipTo(tester);
    }

    @Override
    public boolean skipTo(@NotNull StopCharTester tester) {
        return delegatedCommon.skipTo(tester);
    }

    @NotNull
    @Override
    public MutableDecimal parseDecimal(@NotNull MutableDecimal decimal) {
        return delegatedCommon.parseDecimal(decimal);
    }

    @NotNull
    @Override
    public Excerpt toStart() {
        if (delegatedTailer == null) {
            delegatedExcerpt.toStart();
        } else {
            delegatedTailer.toStart();
        }

        return this;
    }

    @NotNull
    @Override
    public WrappedExcerpt toEnd() {
        delegatedCommon.toEnd();
        return this;
    }

    @Override
    public boolean isFinished() {
        return delegatedCommon.isFinished();
    }

    @Override
    public boolean wasPadding() {
        return delegatedCommon.wasPadding();
    }

    @NotNull
    @Override
    public ByteStringAppender appendTimeMillis(long timeInMS) {
        delegatedCommon.appendTimeMillis(timeInMS);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender appendDateMillis(long timeInMS) {
        delegatedCommon.appendDateMillis(timeInMS);
        return this;
    }

    @NotNull
    @Override
    public ByteStringAppender appendDateTimeMillis(long timeInMS) {
        delegatedCommon.appendDateTimeMillis(timeInMS);
        return this;
    }

    @NotNull
    @Override
    public <E> ByteStringAppender append(@NotNull Iterable<E> list, @NotNull CharSequence seperator) {
        delegatedCommon.append(list, seperator);
        return this;
    }

    @Override
    public int readVolatileInt() {
        return delegatedCommon.readVolatileInt();
    }

    @Override
    public int readVolatileInt(long offset) {
        return delegatedCommon.readVolatileInt(offset);
    }

    @Override
    public long readVolatileLong() {
        return delegatedCommon.readVolatileLong();
    }

    @Override
    public long readVolatileLong(long offset) {
        return delegatedCommon.readVolatileLong(offset);
    }

    @Override
    public void writeOrderedInt(int v) {
        delegatedCommon.writeOrderedInt(v);
    }

    @Override
    public void writeOrderedInt(long offset, int v) {
        delegatedCommon.writeOrderedInt(offset, v);
    }

    @Override
    public boolean compareAndSwapInt(long offset, int expected, int x) {
        return delegatedCommon.compareAndSwapInt(offset, expected, x);
    }

    @Override
    public int getAndAdd(long offset, int delta) {
        return delegatedCommon.getAndAdd(offset, delta);
    }

    @Override
    public int addAndGetInt(long offset, int delta) {
        return delegatedCommon.addAndGetInt(offset, delta);
    }

    @Override
    public void writeOrderedLong(long v) {
        delegatedCommon.writeOrderedLong(v);
    }

    @Override
    public void writeOrderedLong(long offset, long v) {
        delegatedCommon.writeOrderedLong(offset, v);
    }

    @Override
    public boolean compareAndSwapLong(long offset, long expected, long x) {
        return delegatedCommon.compareAndSwapLong(offset, expected, x);
    }

    @Override
    public WrappedExcerpt position(long position) {
        delegatedCommon.position(position);
        return this;
    }

    @NotNull
    @Override
    public ByteOrder byteOrder() {
        return delegatedCommon.byteOrder();
    }

    @Override
    public void checkEndOfBuffer() throws IndexOutOfBoundsException {
        delegatedCommon.checkEndOfBuffer();
    }

    @Override
    public boolean tryLockInt(long offset) {
        return delegatedCommon.tryLockInt(offset);
    }

    @Override
    public boolean tryLockNanosInt(long offset, long nanos) {
        return delegatedCommon.tryLockNanosInt(offset, nanos);
    }

    @Override
    public void busyLockInt(long offset) throws InterruptedException, IllegalStateException {
        delegatedCommon.busyLockInt(offset);
    }

    @Override
    public void unlockInt(long offset) throws IllegalStateException {
        delegatedCommon.unlockInt(offset);
    }

    @Override
    public boolean tryLockLong(long offset) {
        return delegatedCommon.tryLockLong(offset);
    }

    @Override
    public boolean tryLockNanosLong(long offset, long nanos) {
        return delegatedCommon.tryLockNanosLong(offset, nanos);
    }

    @Override
    public void busyLockLong(long offset) throws InterruptedException, IllegalStateException {
        delegatedCommon.busyLockLong(offset);
    }

    @Override
    public void unlockLong(long offset) throws IllegalStateException {
        delegatedCommon.unlockLong(offset);
    }

    @Override
    public float readVolatileFloat(long offset) {
        return delegatedCommon.readVolatileFloat(offset);
    }

    @Override
    public double readVolatileDouble(long offset) {
        return delegatedCommon.readVolatileDouble(offset);
    }

    @Override
    public void writeOrderedFloat(long offset, float v) {
        delegatedCommon.writeOrderedFloat(offset, v);
    }

    @Override
    public void writeOrderedDouble(long offset, double v) {
        delegatedCommon.writeOrderedDouble(offset, v);
    }

    @Override
    public byte addByte(long offset, byte b) {
        return delegatedCommon.addByte(offset, b);
    }

    @Override
    public int addUnsignedByte(long offset, int i) {
        return delegatedCommon.addUnsignedByte(offset, i);
    }

    @Override
    public short addShort(long offset, short s) {
        return delegatedCommon.addShort(offset, s);
    }

    @Override
    public int addUnsignedShort(long offset, int i) {
        return delegatedCommon.addUnsignedShort(offset, i);
    }

    @Override
    public int addInt(long offset, int i) {
        return delegatedCommon.addInt(offset, i);
    }

    @Override
    public long addUnsignedInt(long offset, long i) {
        return delegatedCommon.addUnsignedInt(offset, i);
    }

    @Override
    public long addLong(long offset, long i) {
        return delegatedCommon.addLong(offset, i);
    }

    @Override
    public float addFloat(long offset, float f) {
        return delegatedCommon.addFloat(offset, f);
    }

    @Override
    public double addDouble(long offset, double d) {
        return delegatedCommon.addDouble(offset, d);
    }

    @Override
    public int addAtomicInt(long offset, int i) {
        return delegatedCommon.addAtomicInt(offset, i);
    }

    @Override
    public long addAtomicLong(long offset, long l) {
        return delegatedCommon.addAtomicLong(offset, l);
    }

    @Override
    public float addAtomicFloat(long offset, float f) {
        return delegatedCommon.addAtomicFloat(offset, f);
    }

    @Override
    public double addAtomicDouble(long offset, double d) {
        return delegatedCommon.addAtomicDouble(offset, d);
    }

    @Override
    public long findMatch(@NotNull ExcerptComparator comparator) {
        return delegatedExcerpt.findMatch(comparator);
    }

    @Override
    public void findRange(@NotNull long[] startEnd, @NotNull ExcerptComparator comparator) {
        delegatedExcerpt.findRange(startEnd, comparator);
    }

    @NotNull
    @Override
    public ByteStringAppender append(long l, int base) {
        delegatedCommon.append(l, base);
        return this;
    }

    @Override
    public long parseLong(int base) {
        return delegatedCommon.parseLong(base);
    }

    @Override
    public void write(RandomDataInput bytes, long position, long length) {
        delegatedCommon.write(bytes, position, length);
    }

    @Override
    public void readMarshallable(@NotNull Bytes in) throws IllegalStateException {
        delegatedCommon.readMarshallable(in);
    }

    @Override
    public void writeMarshallable(@NotNull Bytes out) {
        delegatedCommon.writeMarshallable(out);
    }

    @Override
    public int length() {
        return delegatedCommon.length();
    }

    @Override
    public char charAt(int index) {
        return delegatedCommon.charAt(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return delegatedCommon.subSequence(start, end);
    }

    @Override
    public Bytes flip() {
        return delegatedCommon.flip();
    }

    @NotNull
    @Override
    public <T> T readInstance(@NotNull Class<T> objClass, T obj) {
        return delegatedCommon.readInstance(objClass, obj);
    }

    @Override
    public boolean startsWith(RandomDataInput keyBytes) {
        return delegatedCommon.startsWith(keyBytes);
    }

    @Override
    public void write(RandomDataInput bytes) {
        delegatedCommon.write(bytes);
    }

    @Override
    public <OBJ> void writeInstance(@NotNull Class<OBJ> objClass, @NotNull OBJ obj) {
        delegatedCommon.writeInstance(objClass, obj);
    }

    @Override
    public Bytes zeroOut() {
        delegatedCommon.zeroOut();
        return this;
    }

    @Override
    public Bytes zeroOut(long start, long end) {
        delegatedCommon.zeroOut(start, end);
        return this;
    }

    @Override
    public long limit() {
        return delegatedCommon.limit();
    }

    @Override
    public Bytes limit(long limit) {
        delegatedCommon.limit(limit);
        return this;
    }

    @Override
    public Bytes load() {
        delegatedCommon.load();
        return this;
    }

    @Override
    public Bytes slice() {
        return delegatedCommon.slice();
    }

    @Override
    public Bytes slice(long offset, long length) {
        return delegatedCommon.slice(offset, length);
    }

    @Override
    public Bytes bytes() {
        return delegatedCommon.bytes();
    }

    @Override
    public Bytes bytes(long offset, long length) {
        return delegatedCommon.bytes(offset, length);
    }

    @Override
    public long address() {
        return delegatedCommon.address();
    }

    @Override
    public void free() {
        delegatedCommon.free();
    }

    @Override
    public void resetLockInt(long offset) {
        delegatedCommon.resetLockInt(offset);
    }

    @Override
    public int threadIdForLockInt(long offset) {
        return delegatedCommon.threadIdForLockInt(offset);
    }

    @Override
    public void resetLockLong(long offset) {
        delegatedCommon.resetLockLong(offset);
    }

    @Override
    public long threadIdForLockLong(long offset) {
        return delegatedCommon.threadIdForLockLong(offset);
    }

    @Override
    public void reserve() {
        delegatedCommon.reserve();
    }

    @Override
    public void release() {
        delegatedCommon.release();
    }

    @Override
    public int refCount() {
        return delegatedCommon.refCount();
    }

    @Override
    public void toString(Appendable sb, long start, long position, long end) {
        delegatedCommon.toString(sb, start, position, end);
    }

    @Override
    public void alignPositionAddr(int alignment) {
        delegatedCommon.alignPositionAddr(alignment);
    }

    @Override
    public void asString(Appendable appendable) {
        delegatedCommon.asString(appendable);
    }

    @Override
    public CharSequence asString() {
        return delegatedCommon.asString();
    }

    @Override
    public void selfTerminating(boolean selfTerminate) {
        delegatedCommon.selfTerminating(selfTerminate);
    }

    @Override
    public boolean selfTerminating() {
        return delegatedCommon.selfTerminating();
    }

    @Override
    public int readUnsignedByteOrThrow() throws BufferUnderflowException {
        return delegatedCommon.readUnsignedByteOrThrow();
    }

    public String toDebugString() {
        return delegatedCommon.toDebugString();
    }

    @Override
    public boolean compareAndSwapDouble(long offset, double expected, double x) {
        return delegatedCommon.compareAndSwapDouble(offset, expected, x);
    }

    @Override
    public File file() {
        return delegatedCommon.file();
    }
}
