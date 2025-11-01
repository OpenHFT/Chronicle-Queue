/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.main;

import net.openhft.chronicle.queue.internal.main.InternalDumpMain;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

/**
 * DumpMain is an entry point for dumping the contents of a Chronicle Queue file.
 * <p>This class uses several system properties to configure the dumping process:
 * <ul>
 *   <li><b>file</b>: Specifies the file to be dumped</li>
 *   <li><b>skipTableStoreDump</b>: Set to true to skip dumping the TableStore</li>
 *   <li><b>dumpUnaligned</b>: Set to true to dump unaligned data</li>
 * </ul>
 * These properties can be set using JVM system properties when running the application.
 */
public final class DumpMain {

    /**
     * The main method that triggers the dump process.
     * Delegates the execution to {@link InternalDumpMain#main(String[])}.
     *
     * @param args Command-line arguments
     * @throws FileNotFoundException if the specified file is not found
     */
    public static void main(String[] args) throws FileNotFoundException {
        InternalDumpMain.main(args);
    }

    /**
     * Dumps the contents of a Chronicle Queue file located at the specified path.
     *
     * @param path The path to the Chronicle Queue file
     * @throws FileNotFoundException if the specified file is not found
     */
    public static void dump(@NotNull String path) throws FileNotFoundException {
        InternalDumpMain.dump(path);
    }

    /**
     * Dumps the contents of a Chronicle Queue file to the specified {@link PrintStream}.
     * <p>This method provides more fine-grained control over the output, including setting an upper limit for the dump.
     *
     * @param path       The Chronicle Queue file to be dumped
     * @param out        The {@link PrintStream} to which the dump will be written
     * @param upperLimit The upper limit for the dump, controlling how much of the file is dumped
     */
    public static void dump(@NotNull File path, @NotNull PrintStream out, long upperLimit) {
        InternalDumpMain.dump(path, out, upperLimit);
    }
}
