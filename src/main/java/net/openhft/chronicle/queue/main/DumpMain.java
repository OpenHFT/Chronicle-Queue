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

package net.openhft.chronicle.queue.main;

import net.openhft.chronicle.queue.internal.main.InternalDumpMain;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

/**
 * Parameters to the methods in this class can be set using any of the
 * following system properties:
 * <p>
 * private static final String FILE = System.getProperty("file");
 * private static final boolean SKIP_TABLE_STORE = Jvm.getBoolean("skipTableStoreDump");
 * private static final boolean UNALIGNED = Jvm.getBoolean("dumpUnaligned");
 */
public final class DumpMain {

    public static void main(String[] args) throws FileNotFoundException {
        InternalDumpMain.main(args);
    }

    public static void dump(@NotNull String path) throws FileNotFoundException {
        InternalDumpMain.dump(path);
    }

    public static void dump(@NotNull File path, @NotNull PrintStream out, long upperLimit) {
        InternalDumpMain.dump(path, out, upperLimit);
    }
}
