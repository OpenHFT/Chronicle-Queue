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
import sun.nio.ch.DirectBuffer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class VanillaFile implements Closeable {
    private final Logger logger;
    private final String filename;
    private final FileChannel fc;
    private final MappedByteBuffer map;
    private final NativeBytes bytes;
    private final AtomicInteger usage = new AtomicInteger(1);
    private volatile boolean closed = false;

    public VanillaFile(String basePath, String cycleStr, String name, long size) throws IOException {
        logger = Logger.getLogger(VanillaFile.class.getName() + "." + name);
        String dir = basePath + File.separatorChar + cycleStr;
        File dirFile = new File(dir);
        if (!dirFile.isDirectory()) {
            boolean created = dirFile.mkdirs();
            logger.info("Created " + dirFile + " is " + created);
        }
        filename = dir + File.separatorChar + name;
//        logger.info((new File(filename).exists() ? "Creating " : "Opening ") + filename);
        fc = new RandomAccessFile(filename, "rw").getChannel();
        map = fc.map(FileChannel.MapMode.READ_WRITE, 0, size);
        map.order(ByteOrder.nativeOrder());
        long address = ((DirectBuffer) map).address();
        bytes = new NativeBytes(null, address, address, address + size);
    }

    public String filename() {
        return filename;
    }

    public NativeBytes bytes() {
        return bytes;
    }

    public void incrementUsage() {
        usage.incrementAndGet();
    }

    public void decrementUsage() {
        if (usage.decrementAndGet() <= 0 && closed)
            close0();
    }

    public int usage() {
        return usage.get();
    }

    private void close0() {
        try {
            fc.close();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public void close() {
        if (usage.get() <= 0)
            close0();
        closed = true;
    }
}
