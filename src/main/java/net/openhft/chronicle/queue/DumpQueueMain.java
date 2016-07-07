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

import static java.lang.System.err;
import static java.lang.System.out;

/**
 * Created by Peter on 07/03/2016.
 */
public class DumpQueueMain {
    public static void main(String[] args) {
        dump(args[0]);
    }

    public static void dump(String path) {
        File path2 = new File(path);
        if (path2.isDirectory()) {
            File[] files = path2.listFiles();
            if (files == null)
                err.println("Directory not found " + path);

            for (File file : files)
                dumpFile(file);

        } else {
            dumpFile(path2);
        }
    }

    private static void dumpFile(File file) {
        if (file.getName().endsWith(SingleChronicleQueue.SUFFIX)) {
            try (MappedBytes bytes = MappedBytes.mappedBytes(file, 4 << 20)) {
                bytes.readLimit(bytes.realCapacity());
                out.println(Wires.fromSizePrefixedBlobs(bytes));
            } catch (IOException ioe) {
                err.println("Failed to read " + file + " " + ioe);
            }
        }
    }
}
