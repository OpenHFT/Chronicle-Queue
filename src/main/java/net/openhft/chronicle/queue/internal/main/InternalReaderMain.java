/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.internal.main;

import net.openhft.chronicle.queue.ChronicleReaderMain;
import net.openhft.chronicle.queue.reader.Reader;
import net.openhft.chronicle.wire.WireType;
import org.apache.commons.cli.*;
import org.jetbrains.annotations.NotNull;

import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.function.Consumer;

import static java.util.Arrays.stream;

/**
 * Display records in a Chronicle in a text form.
 */
// This class just extends ChronicleReaderMain which might get deprecated later on.
// Once deprecated and before ChronicleReaderMain is subsequently removed, that class
// may be copied to this class.
public class InternalReaderMain extends ChronicleReaderMain {

    public static void main(@NotNull String[] args) {
        new InternalReaderMain().run(args);
    }

}