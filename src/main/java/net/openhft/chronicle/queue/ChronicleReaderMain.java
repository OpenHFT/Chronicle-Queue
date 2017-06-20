/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
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

package net.openhft.chronicle.queue;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;

/**
 * Display records in a Chronicle in a text form.
 *
 * @author peter.lawrey
 */
public enum ChronicleReaderMain {
    ;

    public static void main(@NotNull String[] args) throws IOException {

        final Options options = options();
        final CommandLine commandLine = parseCommandLine(args, options);

        final ChronicleReader chronicleReader = new ChronicleReader().
                withBasePath(Paths.get(commandLine.getOptionValue('d')));

        configureReader(options, chronicleReader);

        chronicleReader.execute();
    }

    private static CommandLine parseCommandLine(final @NotNull String[] args, final Options options) {
        final CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException e) {
            new HelpFormatter().printUsage(new PrintWriter(System.out), 120,
                    ChronicleReaderMain.class.getSimpleName(), options);
            System.exit(1);
        }
        return commandLine;
    }

    private static void configureReader(final Options options, final ChronicleReader chronicleReader) {
        if (options.hasOption("i")) {
            chronicleReader.withInclusionRegex(options.getOption("i").getValue());
        }
        if (options.hasOption("e")) {
            chronicleReader.withExclusionRegex(options.getOption("e").getValue());
        }
        if (options.hasOption("f")) {
            chronicleReader.tail();
        }
        if (options.hasOption("m")) {
            chronicleReader.historyRecords(Long.parseLong(options.getOption("m").getValue()));
        }
        if (options.hasOption("n")) {
            chronicleReader.withStartIndex(Long.decode(options.getOption("n").getValue()));
        }
    }

    @NotNull
    private static Options options() {
        final Options options = new Options();
        options.addRequiredOption("d", "directory", true, "Directory containing chronicle queue files");
        options.addOption("i", "include-regex", true, "Display records containing this regular expression");
        options.addOption("e", "exclude-regex", true, "Do not display records containing this regular expression");
        options.addOption("f", "follow", false, "Tail behaviour - wait for new records to arrive");
        options.addOption("m", "max-history", true, "Show this many records from the end of the data set");
        options.addOption("n", "from-index", true, "Start reading from this index (e.g. 0x123ABE)");
        return options;
    }
}