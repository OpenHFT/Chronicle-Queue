/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.wire.Wires;

import java.io.File;
import java.io.IOException;

/**
 * Created by Peter on 07/03/2016.
 */
public class DumpQueueMain {
    public static void main(String[] args) {
        dump(args[0]);
    }

    public static void dump(String dir) {
        File[] files = new File(dir).listFiles();
        if (files == null) {
            System.err.println("Directory not found " + dir);
        }
        for (File file : files) {
            if (file.getName().endsWith(SingleChronicleQueue.SUFFIX)) {
                try (MappedBytes bytes = MappedBytes.mappedBytes(file, 4 << 20)) {
                    System.out.println(Wires.fromSizePrefixedBlobs(bytes));
                } catch (IOException ioe) {
                    System.err.println("Failed to read " + file + " " + ioe);
                }
            }
        }
    }
}
