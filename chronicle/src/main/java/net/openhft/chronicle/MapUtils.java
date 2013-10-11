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

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * User: peter.lawrey Date: 13/08/13 Time: 18:58
 */
public class MapUtils {
    public static MappedByteBuffer getMap(@NotNull FileChannel fileChannel, long start, int size) throws IOException {
        for (int i = 1; ; i++) {
            try {
//                long startTime = System.nanoTime();
                @SuppressWarnings("UnnecessaryLocalVariable")
                MappedByteBuffer map = fileChannel.map(FileChannel.MapMode.READ_WRITE, start, size);
//                long time = System.nanoTime() - startTime;
//                System.out.printf("Took %,d us to map %,d MB%n", time / 1000, size / 1024 / 1024);
//                System.out.println("Map size: "+size);
                return map;
            } catch (IOException e) {
                if (e.getMessage() == null || !e.getMessage().endsWith("user-mapped section open")) {
                    throw e;
                }
                if (i < 10)
                    //noinspection CallToThreadYield
                    Thread.yield();
                else
                    try {
                        //noinspection BusyWait
                        Thread.sleep(1);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                        throw e;
                    }
            }
        }
    }
}
