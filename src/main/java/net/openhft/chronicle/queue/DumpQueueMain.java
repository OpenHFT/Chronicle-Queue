/*
 * Copyright 2016-2020 http://chronicle.software
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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.table.SingleTableStore;
import net.openhft.chronicle.wire.WireDumper;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static java.lang.System.err;

public class DumpQueueMain {
    private static final String FILE = System.getProperty("file");
    private static final Boolean SKIP_TABLE_STORE = Boolean.getBoolean("skipTableStoreDump");
    private static final Boolean UNALIGNED = Boolean.getBoolean("dumpUnaligned");
    private static final int LENGTH = ", 0".length();

    public static void main(String[] args) throws FileNotFoundException {
        dump(args[0]);
    }

    public static void dump(@NotNull String path) throws FileNotFoundException {
        File path2 = new File(path);
        PrintStream out = FILE == null ? System.out : new PrintStream(new File(FILE));
        long upperLimit = Long.MAX_VALUE;
        dump(path2, out, upperLimit);
    }

    public static void dump(@NotNull File path, @NotNull PrintStream out, long upperLimit) {
        if (path.isDirectory()) {
            final FilenameFilter filter =
                    SKIP_TABLE_STORE
                            ? (d, n) -> n.endsWith(SingleChronicleQueue.SUFFIX)
                            : (d, n) -> n.endsWith(SingleChronicleQueue.SUFFIX) || n.endsWith(SingleTableStore.SUFFIX);
            File[] files = path.listFiles(filter);
            if (files == null) {
                err.println("Directory not found " + path);
                System.exit(1);
            }

            Arrays.sort(files);
            for (File file : files)
                dumpFile(file, out, upperLimit);

        } else if (path.getName().endsWith(SingleChronicleQueue.SUFFIX) || path.getName().endsWith(SingleTableStore.SUFFIX)) {
            dumpFile(path, out, upperLimit);
        }
    }

    private static void dumpFile(@NotNull File file, @NotNull PrintStream out, long upperLimit) {
        Bytes<ByteBuffer> buffer = Bytes.elasticByteBuffer();
        try {
            MappedBytes bytes = MappedBytes.mappedBytes(file, 4 << 20, OS.pageSize(), !OS.isWindows());
            bytes.readLimit(bytes.realCapacity());
            StringBuilder sb = new StringBuilder();
            WireDumper dumper = WireDumper.of(bytes, !UNALIGNED);
            while (bytes.readRemaining() >= 4) {
                sb.setLength(0);
                boolean last = dumper.dumpOne(sb, buffer);
                if (sb.indexOf("\nindex2index:") != -1 || sb.indexOf("\nindex:") != -1) {
                    // truncate trailing zeros
                    if (sb.indexOf(", 0\n]\n") == sb.length() - 6) {
                        int i = indexOfLastZero(sb);
                        if (i < sb.length())
                            sb.setLength(i - 5);
                        sb.append(" # truncated trailing zeros\n]");
                    }
                }

                out.println(sb);

                if (last)
                    break;
                if (bytes.readPosition() > upperLimit) {
                    out.println("# limit reached.");
                    return;
                }
            }
        } catch (IOException ioe) {
            err.println("Failed to read " + file + " " + ioe);
        } finally {
            buffer.release();
        }
    }

    private static int indexOfLastZero(@NotNull CharSequence str) {
        int i = str.length() - 3;
        do {
            i -= LENGTH;
            CharSequence charSequence = str.subSequence(i, i + 3);
            if (!", 0".contentEquals(charSequence))
                return i + LENGTH;
        } while (i > 3);
        return 0;
    }
}
