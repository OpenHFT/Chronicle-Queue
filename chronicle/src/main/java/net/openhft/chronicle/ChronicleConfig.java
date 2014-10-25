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

package net.openhft.chronicle;

import net.openhft.lang.Jvm;
import net.openhft.lang.model.constraints.NotNull;

import java.nio.ByteOrder;

/**
 * @author peter.lawrey
 */
public class ChronicleConfig implements Cloneable {

    /**
     * A pre-defined ChronicleConfig for small {@link net.openhft.chronicle.Chronicle} instances.
     * It has the following params:
     * <ul>
     * <li>data block size <b>16M</b></li>
     * <li>index file capacity <b>4K</b></li>
     * <li>index file excerpts <b>2M</b></li>
     * <li>minimise footprint <b>true</b></li>
     * </ul>
     */
    public static final ChronicleConfig SMALL = new ChronicleConfig(4 * 1024, 2 * 1024 * 1024, true, 16 * 1024 * 1024); // 16 billion max, or one per day for 11 years.

    /**
     * A pre-defined ChronicleConfig for medium {@link net.openhft.chronicle.Chronicle} instances.
     * It has the following params:
     * <ul>
     * <li>data block size <b>128M</b></li>
     * <li>index file capacity <b>16K</b></li>
     * <li>index file excerpts <b>16M</b></li>
     * <li>minimise footprint <b>false</b></li>
     * </ul>
     */
    public static final ChronicleConfig MEDIUM = new ChronicleConfig(16 * 1024, 16 * 1024 * 1024, false, 128 * 1024 * 1024); // 256 billion max

    /**
     * A pre-defined ChronicleConfig for large {@link net.openhft.chronicle.Chronicle} instances.
     * It has the following params:
     * <ul>
     * <li>data block size <b>512M</b></li>
     * <li>index file capacity <b>64K</b></li>
     * <li>index file excerpts <b>64M</b></li>
     * <li>minimise footprint <b>false</b></li>
     * </ul>
     */
    public static final ChronicleConfig LARGE = new ChronicleConfig(64 * 1024, 64 * 1024 * 1024, false, 512 * 1024 * 1024); // 4 trillion max

    /**
     * A pre-defined ChronicleConfig for huge {@link net.openhft.chronicle.Chronicle} instances.
     * It has the following params:
     * <ul>
     * <li>data block size <b>512M</b></li>
     * <li>index file capacity <b>4M</b></li>
     * <li>index file excerpts <b>256M</b></li>
     * <li>minimise footprint <b>false</b></li>
     * </ul>
     */
    public static final ChronicleConfig HUGE = new ChronicleConfig(4 * 1024 * 1024, 256 * 1024 * 1024, false, 512 * 1024 * 1024); // 1 quadrillion max

    /**
     * A pre-defined ChronicleConfig for test {@link net.openhft.chronicle.Chronicle} instances.
     * It has the following params:
     * <ul>
     * <li>data block size <b>8K</b></li>
     * <li>index file capacity <b>1M</b></li>
     * <li>index file excerpts <b>8K</b></li>
     * <li>minimise footprint <b>true</b></li>
     * </ul>
     */
    public static final ChronicleConfig TEST = new ChronicleConfig(1024 * 1024, 8 * 1024, true, 8 * 1024); // maximise overhead for testing purposes

    /**
     * The default ChronicleConfig used by various {@link net.openhft.chronicle.Chronicle} implementations if not otherwise specified.
     * On 64 bit JVMs it delegates to {@link net.openhft.chronicle.ChronicleConfig#MEDIUM}, on 32 bit ones to {@link net.openhft.chronicle.ChronicleConfig#SMALL}.
     */
    public static final ChronicleConfig DEFAULT = Jvm.is64Bit() ? MEDIUM : SMALL;

    private int indexFileCapacity;
    private int indexFileExcerpts;
    private boolean minimiseFootprint;
    // optional parameters, turn on for benchmarks.
    private boolean useUnsafe = false;
    private boolean synchronousMode = false;
    private ByteOrder byteOrder = ByteOrder.nativeOrder();
    private int cacheLineSize = 64;
    private int dataBlockSize;
    private int indexBlockSize;
    private int messageCapacity = 128 * 1024;
    private boolean useCheckedExcerpt = false;

    private ChronicleConfig(int indexFileCapacity, int indexFileExcerpts, boolean minimiseFootprint, int dataBlockSize) {
        this.indexFileCapacity = indexFileCapacity;
        this.indexFileExcerpts = indexFileExcerpts;
        this.minimiseFootprint = minimiseFootprint;
        this.dataBlockSize = dataBlockSize;
        this.indexBlockSize = Math.max(4096, this.dataBlockSize / 4);
    }

    /**
     * Sets the capacity to be used for index files. This parameter is used only in
     * <tt>net.openhft.chronicle.sandbox.RollingChronicle</tt>.
     *
     * @param indexFileCapacity the capacity to be used for index files
     *
     * @return the same instance of ChronicleConfig that the method was called upon
     */
    public ChronicleConfig indexFileCapacity(int indexFileCapacity) {
        this.indexFileCapacity = indexFileCapacity;
        return this;
    }

    /**
     * Returns the capacity to be used for index files. This parameter is used only in
     * <tt>net.openhft.chronicle.sandbox.RollingChronicle</tt>.
     *
     * @return the capacity to be used for index files
     */
    public int indexFileCapacity() {
        return indexFileCapacity;
    }

    /**
     * Sets the number of excerpts in the index file. This parameter is used only in
     * <tt>net.openhft.chronicle.sandbox.RollingChronicle</tt>.
     *
     * @param indexFileExcerpts the number of excerpts in the index file
     *
     * @return the same instance of ChronicleConfig that the method was called upon
     */
    public ChronicleConfig indexFileExcerpts(int indexFileExcerpts) {
        this.indexFileExcerpts = indexFileExcerpts;
        return this;
    }

    /**
     * Returns the number of excerpts in the index file. This parameter is used only in
     * <tt>net.openhft.chronicle.sandbox.RollingChronicle</tt>.
     *
     * @return the number of excerpts in the index file
     */
    public int indexFileExcerpts() {
        return indexFileExcerpts;
    }

    /**
     * Sets a flag specifying if the footprint of the backing files should be kept to a minimum or not.
     * This parameter isn't used by any {@link net.openhft.chronicle.Chronicle} implementations.
     *
     * @param minimiseFootprint if the footprint should be kept to a minimum or not
     */
    public void minimiseFootprint(boolean minimiseFootprint) {
        this.minimiseFootprint = minimiseFootprint;
    }

    /**
     * Returns the flag specifying if the footprint of the backing files should be kept
     * to a minimum or not. This parameter isn't used by any {@link net.openhft.chronicle.Chronicle}
     * implementations.
     *
     * @return the flag specifying if the footprint of the backing files should be kept
     * to a minimum or not
     */
    public boolean minimiseFootprint() {
        return minimiseFootprint;
    }

    /**
     * Specifies if Unsafe should be used by the {@link net.openhft.chronicle.Chronicle} instance being configured.
     * Defaults to <b>false</b>. This parameter is not used by any {@link net.openhft.chronicle.Chronicle} implementations.
     *
     * @param useUnsafe the flag specifying if Unsafe should be used or not
     *
     * @return the same instance of ChronicleConfig that the method was called upon
     */
    public ChronicleConfig useUnsafe(boolean useUnsafe) {
        this.useUnsafe = useUnsafe;
        return this;
    }

    /**
     * Returns the flag specifying if Unsafe should be used by the {@link net.openhft.chronicle.Chronicle} instance being configured.
     * Defaults to <b>false</b>. This parameter is not used by any {@link net.openhft.chronicle.Chronicle} implementations.
     *
     * @return the flag specifying if Unsafe should be used or not
     */
    public boolean useUnsafe() {
        return useUnsafe;
    }

    /**
     * Sets the synchronous mode to be used. Enabling synchronous mode means that
     * {@link ExcerptCommon#finish()} will force a persistence every time.
     *
     * @param synchronousMode If synchronous mode should be used or not.
     *
     * @return the same instance of ChronicleConfig that the method was called upon
     */
    public ChronicleConfig synchronousMode(boolean synchronousMode) {
        this.synchronousMode = synchronousMode;
        return this;
    }

    /**
     * Checks if synchronous mode is enabled or not.
     * @return true if synchronous mode is enabled, false otherwise.
     */
    public boolean synchronousMode() {
        return synchronousMode;
    }

    /**
     * Sets the byte order to be used when serializing into/from the backing files. Defaults to <b>native order</b>.
     *
     * @param byteOrder the byte order to be used when serializing (big endian, little endian and such)
     *
     * @return the same instance of ChronicleConfig that the method was called upon
     */
    public ChronicleConfig byteOrder(ByteOrder byteOrder) {
        this.byteOrder = byteOrder;
        return this;
    }

    /**
     * Returns the byte order being used when serializing (into/from the backing files). Defaults to <b>native order</b>.
     *
     * @return the byte order used when serializing
     */
    public ByteOrder byteOrder() {
        return byteOrder;
    }

    /**
     * Sets the size of the index cache lines. Index caches (files) consist of fixed size lines, each line having
     * multiple index entries on it. This param specifies the size of such a multi entry line. Default value is <b>64</b>.
     *
     * @param cacheLineSize the size of the cache lines making up index files
     *
     * @return the same instance of ChronicleConfig that the method was called upon
     */
    public ChronicleConfig cacheLineSize(int cacheLineSize) {
        this.cacheLineSize = cacheLineSize;
        return this;
    }

    /**
     * The size of the index cache lines (index caches are made up of multiple fixed length lines, each line
     * contains multiple index entries). Default value is <b>64</b>.
     *
     * @return the size of the index cache lines
     */
    public int cacheLineSize() {
        return cacheLineSize;
    }

    /**
     * Sets the size to be used for data blocks. The method also has a side effect. If the data
     * block size specified is smaller than twice the message capacity, then the message capacity
     * will be adjusted to equal half of the new data block size.
     *
     * @param dataBlockSize the size of the data blocks
     *
     * @return the same instance of ChronicleConfig that the method was called upon
     */
    public ChronicleConfig dataBlockSize(int dataBlockSize) {
        this.dataBlockSize = dataBlockSize;
        if (messageCapacity > dataBlockSize / 2)
            messageCapacity = dataBlockSize / 2;
        return this;
    }

    /**
     * Returns the size of the data blocks.
     *
     * @return the size of the data blocks
     */
    public int dataBlockSize() {
        return dataBlockSize;
    }

    /**
     * Sets the size to be used for index blocks. Is capped to the bigger value among <b>4096</b> and a
     * <b>quarter of the <tt>data block size</tt></b>.
     *
     * @param indexBlockSize the size of the index blocks
     *
     * @return the same instance of ChronicleConfig that the method was called upon
     */
    public ChronicleConfig indexBlockSize(int indexBlockSize) {
        this.indexBlockSize = indexBlockSize;
        return this;
    }

    /**
     * Returns the size of the index blocks.
     *
     * @return the size of the index blocks
     */
    public int indexBlockSize() {
        return indexBlockSize;
    }

    /**
     * The maximum size a message stored in a {@link net.openhft.chronicle.Chronicle} instance can have.
     * Defaults to <b>128K</b>. Is limited by the <tt>data block size</tt>, can't be bigger than half of
     * the data block size.
     *
     * @param messageCapacity the maximum message size that can be stored
     *
     * @return the same instance of ChronicleConfig that the method was called upon
     */
    public ChronicleConfig messageCapacity(int messageCapacity) {
        this.messageCapacity = messageCapacity;
        return this;
    }

    /**
     * The maximum size of the message that can be stored in the {@link net.openhft.chronicle.Chronicle}
     * instance to be configured. Defaults to <b>128K</b>. Can't be bigger than half of the <tt>data block size</tt>.
     *
     * @return the maximum message size that can be stored
     */
    public int messageCapacity() {
        return messageCapacity;
    }

    //TODO: document
    public boolean useCheckedExcerpt() {
        return useCheckedExcerpt;
    }

    //TODO: document
    public void useCheckedExcerpt(boolean useCheckedExcerpt) {
        this.useCheckedExcerpt = useCheckedExcerpt;
    }

    /**
     * Makes ChronicleConfig cloneable.
     *
     * @return a cloned copy of this ChronicleConfig instance
     */
    @NotNull
    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    @Override
    public ChronicleConfig clone() {
        try {
            return (ChronicleConfig) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }
}
