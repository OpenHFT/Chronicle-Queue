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

import net.openhft.chronicle.queue.reader.ChronicleReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;

import static java.util.Arrays.stream;

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
                withMessageSink(System.out::println).
                withBasePath(Paths.get(commandLine.getOptionValue('d')));

        configureReader(chronicleReader, commandLine);

        chronicleReader.execute();
    }

    private static CommandLine parseCommandLine(final @NotNull String[] args, final Options options) {
        final CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);

            if (commandLine.hasOption('h')) {
                new HelpFormatter().printHelp(ChronicleReaderMain.class.getSimpleName(), options);
                System.exit(0);
            }
        } catch (ParseException e) {
            printUsageAndExit(options);
        }

        if (!commandLine.hasOption('d')) {
            System.out.println("Please specify the directory with -d\n");
            printUsageAndExit(options);
        }

        return commandLine;
    }

    private static void printUsageAndExit(final Options options) {
        final PrintWriter writer = new PrintWriter(System.out);
        new HelpFormatter().printUsage(writer, 180,
                ChronicleReaderMain.class.getSimpleName(), options);
        writer.flush();
        System.exit(1);
    }

    private static void configureReader(final ChronicleReader chronicleReader, final CommandLine commandLine) {
        if (commandLine.hasOption('i')) {
            stream(commandLine.getOptionValues('i')).forEach(chronicleReader::withInclusionRegex);
        }
        if (commandLine.hasOption('e')) {
            stream(commandLine.getOptionValues('e')).forEach(chronicleReader::withExclusionRegex);
        }
        if (commandLine.hasOption('f')) {
            chronicleReader.tail();
        }
        if (commandLine.hasOption('m')) {
            chronicleReader.historyRecords(Long.parseLong(commandLine.getOptionValue('m')));
        }
        if (commandLine.hasOption('n')) {
            chronicleReader.withStartIndex(Long.decode(commandLine.getOptionValue('n')));
        }
        if (commandLine.hasOption('r')) {
            chronicleReader.asMethodReader();
        }
    }

    @NotNull
    private static Options options() {
        final Options options = new Options();

        addOption(options, "d", "directory", true, "Directory containing chronicle queue files", false);
        addOption(options, "i", "include-regex", true, "Display records containing this regular expression", false);
        addOption(options, "e", "exclude-regex", true, "Do not display records containing this regular expression", false);
        addOption(options, "f", "follow", false, "Tail behaviour - wait for new records to arrive", false);
        addOption(options, "m", "max-history", true, "Show this many records from the end of the data set", false);
        addOption(options, "n", "from-index", true, "Start reading from this index (e.g. 0x123ABE)", false);
        addOption(options, "r", "as-method-reader", false, "Use when reading from a queue generated using a MethodWriter", false);
        addOption(options, "h", "help-message", false, "Print this help and exit", false);
        return options;
    }

    private static void addOption(final Options options, final String opt, final String argName, final boolean hasArg,
                                  final String description, final boolean isRequired) {
        final Option option = new Option(opt, hasArg, description);
        option.setArgName(argName);
        option.setRequired(isRequired);
        options.addOption(option);
    }
}