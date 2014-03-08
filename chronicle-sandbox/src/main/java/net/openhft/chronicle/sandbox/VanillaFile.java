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

package net.openhft.chronicle.sandbox;

import net.openhft.lang.io.NativeBytes;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.*;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class VanillaFile implements Closeable {
    private final Logger logger;
    private final File file;
    private final FileChannel fc;
    private final MappedByteBuffer map;
    private final long baseAddr;
    private final NativeBytes bytes;
    private final AtomicInteger usage = new AtomicInteger(1);
    private final int indexCount;
    private volatile boolean closed = false;

    public VanillaFile(String basePath, String cycleStr, String name, int indexCount, long size, boolean forAppend) throws IOException {
        logger = Logger.getLogger(VanillaFile.class.getName() + "." + name);
        File dir = new File(basePath, cycleStr);
        this.indexCount = indexCount;
        if (!dir.isDirectory()) {
            boolean created = dir.mkdirs();
            if (logger.isLoggable(Level.FINE))
                logger.fine("Created " + dir + " is " + created);
        }
        file = new File(dir, name);
        if (file.exists()) {
            if (logger.isLoggable(Level.FINE))
                logger.fine("Opening " + file);
        } else if (forAppend) {
            if (logger.isLoggable(Level.FINE))
                logger.fine("Creating " + file);
        } else {
            throw new FileNotFoundException(file.getAbsolutePath());
        }
        fc = new RandomAccessFile(file, "rw").getChannel();
        map = fc.map(FileChannel.MapMode.READ_WRITE, 0, size);
        map.order(ByteOrder.nativeOrder());
        baseAddr = ((DirectBuffer) map).address();
        bytes = new NativeBytes(null, baseAddr, baseAddr + size, usage);
    }

    public File file() {
        return file;
    }

    public NativeBytes bytes() {
        return bytes;
    }

    public void incrementUsage() {
        usage.incrementAndGet();
    }

    public void decrementUsage() {
//        new Throwable("dec " + this +" as "+usage).printStackTrace();
        if (usage.decrementAndGet() <= 0 && closed)
            close0();
    }

    public int indexCount() {
        return indexCount;
    }

    public int usage() {
        return usage.get();
    }

    public long baseAddr() {
        return baseAddr;
    }

    private void close0() {
        Logger logger = Logger.getLogger(getClass().getName());
        if (logger.isLoggable(Level.FINE))
            logger.fine("... Closing " + file);
        Cleaner cleaner = ((DirectBuffer) map).cleaner();
        if (cleaner != null)
            cleaner.clean();
        try {
            fc.close();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public void close() {
        closed = true;
        decrementUsage();
    }

    public void force() {
        map.force();
    }
}
