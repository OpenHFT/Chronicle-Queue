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

package net.openhft.chronicle.queue.internal.main;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.impl.table.SingleTableStore;
import net.openhft.chronicle.wire.WireDumper;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static java.lang.System.err;

/**
 * The InternalDumpMain class provides methods to dump the content of Chronicle Queue and Table Store files.
 * It outputs the content either to the standard output or to a specified file.
 * The dump includes detailed binary structure of the files in a human-readable format.
 */
public class InternalDumpMain {

    // File system properties for dumping files
    private static final String FILE = System.getProperty("file");
    private static final boolean SKIP_TABLE_STORE = Jvm.getBoolean("skipTableStoreDump");
    private static final boolean UNALIGNED = Jvm.getBoolean("dumpUnaligned");
    private static final int LENGTH = ", 0".length();

    static {
        // Register aliases for SingleChronicleQueueBuilder
        SingleChronicleQueueBuilder.addAliases();
    }

    /**
     * Main entry point for dumping Chronicle Queue or Table Store files.
     *
     * @param args Command-line arguments where the first argument is the path of the directory or file to be dumped
     * @throws FileNotFoundException if the provided file path is invalid
     */
    public static void main(String[] args) throws FileNotFoundException {
        dump(args[0]);  // Dump the contents of the provided path
    }

    /**
     * Dumps the content of a Chronicle Queue or Table Store file at the given path.
     * Outputs to the file specified by the "file" system property, or to standard output if not specified.
     *
     * @param path Path to the file or directory to be dumped
     * @throws FileNotFoundException if the specified file or directory is not found
     */
    public static void dump(@NotNull String path) throws FileNotFoundException {
        File path2 = new File(path);
        // PrintStream to the specified file, or stdout if no file specified
        PrintStream out = FILE == null ? System.out : new PrintStream(FILE);
        long upperLimit = Long.MAX_VALUE;
        dump(path2, out, upperLimit);  // Dump the file with an upper limit
    }

    /**
     * Dumps the content of files in the provided directory or dumps a single file.
     * Files are printed in order if it's a directory.
     *
     * @param path       Path to the directory or file to dump
     * @param out        PrintStream to output the dump results
     * @param upperLimit Maximum number of bytes to read and dump
     */
    public static void dump(@NotNull File path, @NotNull PrintStream out, long upperLimit) {
        if (path.isDirectory()) {
            // Filter files to dump: based on file suffix (".cq4" for queue, ".tq4" for table store)
            final FilenameFilter filter =
                    SKIP_TABLE_STORE
                            ? (d, n) -> n.endsWith(SingleChronicleQueue.SUFFIX)
                            : (d, n) -> n.endsWith(SingleChronicleQueue.SUFFIX) || n.endsWith(SingleTableStore.SUFFIX);
            File[] files = path.listFiles(filter);
            if (files == null) {
                err.println("Directory not found " + path);
                System.exit(1);
            }

            Arrays.sort(files);  // Sort the files by name
            for (File file : files) {
                out.println("## " + file);
                dumpFile(file, out, upperLimit);  // Dump each file
            }

        } else if (path.getName().endsWith(SingleChronicleQueue.SUFFIX) || path.getName().endsWith(SingleTableStore.SUFFIX)) {
            dumpFile(path, out, upperLimit);  // Dump a single file
        }
    }

    /**
     * Dumps the content of a single Chronicle Queue or Table Store file.
     * Outputs each entry in a human-readable format.
     *
     * @param file       The file to dump
     * @param out        The PrintStream to output the file content to
     * @param upperLimit Maximum number of bytes to dump
     */
    private static void dumpFile(@NotNull File file, @NotNull PrintStream out, long upperLimit) {
        Bytes<ByteBuffer> buffer = Bytes.elasticByteBuffer();  // Temporary buffer for reading file contents
        try (MappedBytes bytes = MappedBytes.mappedBytes(file, 4 << 20, OS.pageSize(), !OS.isWindows())) {
            bytes.readLimit(bytes.realCapacity());  // Set the read limit to the file's actual capacity
            StringBuilder sb = new StringBuilder();  // StringBuilder to hold the dumped output
            WireDumper dumper = WireDumper.of(bytes, !UNALIGNED);  // Create a WireDumper for reading the file
            while (bytes.readRemaining() >= 4) {
                sb.setLength(0);  // Clear the StringBuilder
                boolean last = dumper.dumpOne(sb, buffer);  // Dump one entry into the StringBuilder
                if (sb.indexOf("\nindex2index:") != -1 || sb.indexOf("\nindex:") != -1) {
                    // Truncate trailing zeros for readability
                    if (sb.indexOf(", 0\n]\n") == sb.length() - 6) {
                        int i = indexOfLastZero(sb);
                        if (i < sb.length())
                            sb.setLength(i - 5);
                        sb.append(" # truncated trailing zeros\n]");
                    }
                }

                out.println(sb);  // Print the dumped entry

                if (last)  // Stop if it was the last entry
                    break;
                if (bytes.readPosition() > upperLimit) {
                    out.println("# limit reached.");
                    return;
                }
            }
        } catch (IOException ioe) {
            err.println("Failed to read " + file + " " + ioe);  // Handle I/O exceptions
        } finally {
            buffer.releaseLast();  // Release the buffer
        }
    }

    /**
     * Finds the index of the last zero-value entry in the given character sequence.
     * Used to truncate trailing zeros for better readability in the dump.
     *
     * @param str The CharSequence to search for trailing zeros
     * @return The index where the last trailing zero was found
     */
    private static int indexOfLastZero(@NotNull CharSequence str) {
        int i = str.length() - 3;
        do {
            i -= LENGTH;
            CharSequence charSequence = str.subSequence(i, i + 3);  // Check if the sequence is ", 0"
            if (!", 0".contentEquals(charSequence))
                return i + LENGTH;
        } while (i > 3);
        return 0;
    }
}
