/*
 * Copyright 2013 Peter Lawrey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle;

import net.openhft.lang.io.VanillaMappedBlocks;
import net.openhft.lang.io.VanillaMappedBytes;
import net.openhft.lang.model.constraints.NotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * IndexedChronicle is a single-writer-multiple-reader
 * {@link net.openhft.chronicle.Chronicle} that you can put huge numbers of objects in,
 * having different sizes.
 *
 * <p>For each record, IndexedChronicle holds the memory-offset in another index cache
 * for random access. This means IndexedChronicle "knows" where the Nth object resides at
 * in memory, thus the name "Indexed". But this index is just sequential index,
 * first object has index 0, second object has index 1, and so on. If you want to access
 * objects with other logical keys you have to manage your own mapping from logical key
 * to index.</p>
 *
 * Indexing and data storage are achieved using two backing (memory-mapped) files:
 * <ul>
 * <li>a data file called &#60;base file name&#62;.data</li>
 * <li>an index file called &#60;base file name&#62;.index</li>
 * </ul>
 * , <tt>base file name</tt> (or <tt>basePath</tt>) is provided on construction.
 *
 * @author peter.lawrey
 */
public class IndexedChronicle implements Chronicle {

    @NotNull
    final VanillaMappedBlocks indexFileCache;
    @NotNull
    final VanillaMappedBlocks dataFileCache;

    @NotNull
    final ChronicleConfig config;
    private final String basePath;
    // todo consider making volatile to help detect bugs in calling code.
    private long lastWrittenIndex = -1;
    private volatile boolean closed = false;

    /**
     * Creates a new instance of IndexedChronicle having the specified <tt>basePath</tt> (the base name
     * of the two backing files).
     *
     * @param basePath the base name of the files backing this IndexChronicle
     *
     * @throws FileNotFoundException if the <tt>basePath</tt> string does not denote an existing,
     * writable regular file and a new regular file of that name cannot be created, or if some
     * other error occurs while opening or creating the file
     */
    public IndexedChronicle(@NotNull String basePath) throws IOException {
        this(basePath, ChronicleConfig.DEFAULT);
    }

    /**
     * Creates a new instance of IndexedChronicle as specified by the provided
     * {@link net.openhft.chronicle.ChronicleConfig} and having the specified <tt>basePath</tt> (the base name
     * of the two backing files).
     *
     * @param basePath the base name of the files backing this IndexChronicle
     * @param config the ChronicleConfig based on which the current IndexChronicle should be constructed
     *
     * @throws FileNotFoundException if the <tt>basePath</tt> string does not denote an existing,
     * writable regular file and a new regular file of that name cannot be created, or if some
     * other error occurs while opening or creating the file
     */
    public IndexedChronicle(@NotNull String basePath, @NotNull ChronicleConfig config) throws IOException {
        this.basePath = basePath;
        this.config = config.clone();

        File parentFile = new File(basePath).getParentFile();
        if (parentFile != null) {
            parentFile.mkdirs();
        }

        this.indexFileCache = VanillaMappedBlocks.readWrite(new File(basePath + ".index"),config.indexBlockSize());
        this.dataFileCache  = VanillaMappedBlocks.readWrite(new File(basePath + ".data" ),config.dataBlockSize());

        findTheLastIndex();
    }

    /**
     * Checks if this instance of IndexedChronicle is closed or not.
     * If closed an {@link java.lang.IllegalStateException} will be thrown.
     *
     * @throws java.lang.IllegalStateException if this IndexChronicle is close
     */
    public void checkNotClosed() {
        if (closed) throw new IllegalStateException(basePath + " is closed");
    }

    /**
     * Returns the {@link net.openhft.chronicle.ChronicleConfig} that has been used to create
     * the current instance of IndexedChronicle
     *
     * @return the ChronicleConfig used to create this IndexChronicle
     */
    public ChronicleConfig config() {
        return config; //todo: would be better to return a copy/clone, because like this the config can be changed from the outside
    }

    /**
     * Returns the index of the most recent {@link net.openhft.chronicle.Excerpt}s previously
     * written into this {@link net.openhft.chronicle.Chronicle}. Basically the same value as
     * returned by {@link IndexedChronicle#lastWrittenIndex()}, but does it by looking at the
     * content of the backing files and figuring it out from there.
     *
     * <p>A side effect of the method is that it also stores the obtained value and it can and
     * will be used by subsequent calls of {@link IndexedChronicle#lastWrittenIndex()}.</p>
     *
     * <p>The constructors of IndexedChronicle automatically call
     * this method so they properly handle the backing file being both empty or non-empty at the
     * start.</p>
     *
     * @return the index of the most recent Excerpt written into this Chronicle
     */
    public long findTheLastIndex() {
        return lastWrittenIndex = findTheLastIndex0();
    }

    private long findTheLastIndex0() {
        long size = 0;

        try {
            size = indexFileCache.size();
        } catch(Exception e) {
            return -1;
        }

        if (size <= 0) {
            return -1;
        }

        int indexBlockSize = config.indexBlockSize();
        for (long block = size / indexBlockSize - 1; block >= 0; block--) {
            VanillaMappedBytes mbb = null;
            try {
                mbb = indexFileCache.acquire(block);
            } catch (IOException e) {
                continue;
            }

            if (block > 0 && mbb.readLong(0) == 0) {
                continue;
            }

            int cacheLineSize = config.cacheLineSize();
            for (int pos = 0; pos < indexBlockSize; pos += cacheLineSize) {
                // if the next line is blank
                if (pos + cacheLineSize >= indexBlockSize || mbb.readLong(pos + cacheLineSize) == 0) {
                    // last cache line.
                    int pos2 = 8;
                    for (pos2 = 8; pos2 < cacheLineSize; pos2 += 4) {
                        if (mbb.readInt(pos + pos2) == 0) {
                            break;
                        }
                    }
                    return (block * indexBlockSize + pos) / cacheLineSize * (cacheLineSize / 4 - 2) + pos2 / 4 - 3;
                }
            }
            return (block + 1) * indexBlockSize / cacheLineSize * (cacheLineSize / 4 - 2);
        }
        return -1;
    }

    /**
     * Returns the number of {@link net.openhft.chronicle.Excerpt}s that have been written
     * into this {@link net.openhft.chronicle.Chronicle}.
     *
     * @return the number of Excerpts previously written into this Chronicle
     */
    @Override
    public long size() {
        return lastWrittenIndex + 1;
    }

    /**
     * Returns the base file name backing this instance of IndexChronicle. Index chronicle uses two files:
     * <ul>
     * <li>a data file called &#60;base file name&#62;.data</li>
     * <li>an index file called &#60;base file name&#62;.index</li>
     * </ul>
     * @return the base file name backing this IndexChronicle
     */
    @Override
    public String name() {
        return basePath;
    }

    /**
     * Closes this instance of IndexedChronicle, including the backing files.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        closed = true;
        this.indexFileCache.close();
        this.dataFileCache.close();
    }

    /**
     * Returns a new instance of {@link net.openhft.chronicle.Excerpt} which can be used
     * for random access to the data stored in this Chronicle.
     *
     * @return new {@link net.openhft.chronicle.Excerpt} for this Chronicle
     *
     * @throws IOException if an I/O error occurs
     */
    @NotNull
    @Override
    public Excerpt createExcerpt() throws IOException {
        return new NativeExcerpt(this);
    }

    /**
     * Returns a new instance of {@link net.openhft.chronicle.ExcerptTailer} which can be used
     * for sequential reads from this Chronicle.
     *
     * @return new {@link net.openhft.chronicle.ExcerptTailer} for this Chronicle
     *
     * @throws IOException if an I/O error occurs
     */
    @NotNull
    @Override
    public ExcerptTailer createTailer() throws IOException {
        return new NativeExcerptTailer(this);
    }

    /**
     * Returns a new instance of {@link net.openhft.chronicle.ExcerptAppender} which can be used
     * for sequential writes into this Chronicle.
     *
     * @return new {@link net.openhft.chronicle.ExcerptAppender} for this Chronicle
     *
     * @throws IOException if an I/O error occurs
     */
    @NotNull
    @Override
    public ExcerptAppender createAppender() throws IOException {
        return new NativeExcerptAppender(this);
    }

    /**
     * Returns the index of the most recent {@link net.openhft.chronicle.Excerpt}s previously
     * written into this {@link net.openhft.chronicle.Chronicle}. Basically <tt>size() - 1</tt>.
     *
     * @return the index of the most recent Excerpt written into this Chronicle
     */
    @Override
    public long lastWrittenIndex() {
        return lastWrittenIndex;
    }

    void incrSize() {
        lastWrittenIndex++;
    }
}
