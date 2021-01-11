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

package net.openhft.chronicle.queue.main;

import net.openhft.chronicle.queue.internal.main.InternalReaderMain;
import net.openhft.chronicle.queue.reader.ChronicleReader;
import net.openhft.chronicle.queue.reader.Reader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.jetbrains.annotations.NotNull;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Display records in a queue in a text form.
 *
 */
public final class ReaderMain {

    public static void main(@NotNull String[] args) {
        InternalReaderMain.main(args);
    }

    /* Todo: Add a builder similar to this before ChronicleReaderMain is deprecated
    interface Builder {

        // These "standard" methods provide base functionality

        BiFunction<String[], Options, CommandLine> standardParser();

        Options standardOptions();

        BiConsumer<Reader, CommandLine> standardConfigurator();

        // These "withers" provide override capabilities

        Builder withReader(Reader reader);

        Builder withParser(BiFunction<? extends String[], ? extends Options, CommandLine> parser);

        Builder withOptions(Options options);

        Builder withConfigurator(BiConsumer<? super Reader, ? super CommandLine> configurator);

        Consumer<String[]> build();

    }
    */

}