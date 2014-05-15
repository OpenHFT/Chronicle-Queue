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

import net.openhft.lang.io.NativeBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

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
        logger = LoggerFactory.getLogger(VanillaFile.class.getName() + "." + name);
        File dir = new File(basePath, cycleStr);
        this.indexCount = indexCount;

        if (!forAppend) {
            //This test needs to be done before any directories are created.
            File f = new File(dir, name);
            if (!f.exists()) {
                throw new FileNotFoundException(f.getAbsolutePath());
            }
        }

        if (!dir.isDirectory()) {
            boolean created = dir.mkdirs();
            logger.trace("Created {} is {}",dir,created);
        }
        file = new File(dir, name);
        if (file.exists()) {
            logger.trace("Opening {}",file);
        } else if (forAppend) {
            logger.trace("Creating {}",file);
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
        LoggerFactory.getLogger(getClass()).trace("... Closing {}", file);
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
