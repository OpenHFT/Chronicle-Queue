/*
 *
 *  *     Copyright (C) 2016  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
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
