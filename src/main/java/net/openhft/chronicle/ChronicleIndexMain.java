/*
 * Copyright ${YEAR} Peter Lawrey
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

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author peter.lawrey
 */
public class ChronicleIndexMain {
    public static void main(String... args) throws IOException, NoSuchFieldException, IllegalAccessException {
        String name = "/mnt/ocz/deleteme";
        new File(name).delete();
        new File(name).deleteOnExit();
        FileChannel raf = new RandomAccessFile(name, "rw").getChannel();
        Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
        theUnsafe.setAccessible(true);
        Unsafe unsafe = (Unsafe) theUnsafe.get(null);
        long offset = 0;
        long start = System.nanoTime();
        for (int k = 0; k < 100; k++) {
            long blockSize = (128 * 4 + 8) * 1024 * 1024;
            MappedByteBuffer map = raf.map(FileChannel.MapMode.READ_WRITE, k * blockSize, blockSize);
            long address = ((DirectBuffer) map).address();
            for (int i = 0; i < 1024 * 1024; i++) {
                offset = i * (128 + 8);
                unsafe.putOrderedLong(null, address + offset, offset);
                offset += 8;
                for (int j = 0; j < 128; j += 4) {
                    offset += 4;
                    unsafe.putOrderedInt(null, address + offset, (int) offset);
                    offset += 4;
                    unsafe.putOrderedInt(null, address + offset, (int) offset);
                    offset += 4;
                    unsafe.putOrderedInt(null, address + offset, (int) offset);
                    offset += 4;
                    unsafe.putOrderedInt(null, address + offset, (int) offset);
                }
            }
        }
        raf.close();
        long time = System.nanoTime() - start;
        System.out.printf("Updated %,d per second%n", 128 * 1024 * 1024 * 1000000000L / time * 100);
    }
}
