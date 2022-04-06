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

package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.reader.ChronicleReader;
import net.openhft.chronicle.queue.reader.ContentBasedLimiter;
import net.openhft.chronicle.wire.AbstractTimestampLongConverter;
import net.openhft.chronicle.wire.WireType;
import org.apache.commons.cli.*;
import org.jetbrains.annotations.NotNull;

import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.util.function.Consumer;

import static java.util.Arrays.stream;

/**
 * Display records in a Chronicle in a text form.
 */
public class ChronicleReaderMain {

    public static void main(@NotNull String[] args) {
        new ChronicleReaderMain().run(args);
    }

    public static void addOption(final Options options,
                                 final String opt,
                                 final String argName,
                                 final boolean hasArg,
                                 final String description,
                                 final boolean isRequired) {
        final Option option = new Option(opt, hasArg, description);
        option.setArgName(argName);
        option.setRequired(isRequired);
        options.addOption(option);
    }

    protected void run(@NotNull String[] args) {
        final Options options = options();
        final CommandLine commandLine = parseCommandLine(args, options);

        final ChronicleReader chronicleReader = chronicleReader();

        configureReader(chronicleReader, commandLine);

        chronicleReader.execute();
    }

    protected ChronicleReader chronicleReader() {
        return new ChronicleReader();
    }

    protected CommandLine parseCommandLine(final @NotNull String[] args, final Options options) {
        final CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);

            if (commandLine.hasOption('h')) {
                printHelpAndExit(options, 0);
            }
        } catch (ParseException e) {
            printHelpAndExit(options, 1, e.getMessage());
        }

        return commandLine;
    }

    protected void printHelpAndExit(final Options options, int status) {
        printHelpAndExit(options, status, null);
    }

    protected void printHelpAndExit(final Options options, int status, String message) {
        final PrintWriter writer = new PrintWriter(System.out);
        new HelpFormatter().printHelp(
                writer,
                180,
                this.getClass().getSimpleName(),
                message,
                options,
                HelpFormatter.DEFAULT_LEFT_PAD,
                HelpFormatter.DEFAULT_DESC_PAD,
                null,
                true
        );
        writer.flush();
        System.exit(status);
    }

    protected void configureReader(final ChronicleReader chronicleReader, final CommandLine commandLine) {
        final Consumer<String> messageSink = commandLine.hasOption('l') ?
                s -> System.out.println(s.replaceAll("\n", "")) :
                System.out::println;
        chronicleReader.
                withMessageSink(messageSink).
                withBasePath(Paths.get(commandLine.getOptionValue('d')));

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
            final String r = commandLine.getOptionValue('r');
            chronicleReader.asMethodReader(r.equals("null") ? null : r);
            chronicleReader.showMessageHistory(commandLine.hasOption('g'));
        }
        if (commandLine.hasOption('w')) {
            chronicleReader.withWireType(WireType.valueOf(commandLine.getOptionValue('w')));
        }
        if (commandLine.hasOption('s')) {
            chronicleReader.suppressDisplayIndex();
        }
        if (commandLine.hasOption('z')) {
            System.setProperty(AbstractTimestampLongConverter.TIMESTAMP_LONG_CONVERTERS_ZONE_ID_SYSTEM_PROPERTY,
                    ZoneId.systemDefault().toString());
        }
        if (commandLine.hasOption('a')) {
            chronicleReader.withArg(commandLine.getOptionValue('a'));
        }
        if (commandLine.hasOption('b')) {
            chronicleReader.withBinarySearch(commandLine.getOptionValue('b'));
        }
        if (commandLine.hasOption('k')) {
            chronicleReader.inReverseOrder();
        }
        if (commandLine.hasOption('x')) {
            chronicleReader.withMatchLimit(Long.parseLong(commandLine.getOptionValue('x')));
        }
        if (commandLine.hasOption("cbl")) {
            final String cbl = commandLine.getOptionValue("cbl");
            try {
                chronicleReader.withContentBasedLimiter((ContentBasedLimiter) Class.forName(cbl).getConstructor().newInstance());
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Error creating content-based limiter, could not find class: " + cbl, e);
            } catch (NoSuchMethodException e) {
                throw new IllegalArgumentException("Error creating content-based limiter, it must have a no-argument constructor", e);
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("Error creating content-based limiter, it must implement " + ContentBasedLimiter.class.getName(), e);
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                throw new IllegalArgumentException("Error creating content-based-limiter class: " + cbl, e);
            }
        }
        if (commandLine.hasOption("cblArg")) {
            chronicleReader.withLimiterArg(commandLine.getOptionValue("cblArg"));
        }
    }

    @NotNull
    protected Options options() {
        final Options options = new Options();

        addOption(options, "d", "directory", true, "Directory containing chronicle queue files", true);
        addOption(options, "i", "include-regex", true, "Display records containing this regular expression", false);
        addOption(options, "e", "exclude-regex", true, "Do not display records containing this regular expression", false);
        addOption(options, "f", "follow", false, "Tail behaviour - wait for new records to arrive", false);
        addOption(options, "m", "max-history", true, "Show this many records from the end of the data set", false);
        addOption(options, "n", "from-index", true, "Start reading from this index (e.g. 0x123ABE)", false);
        addOption(options, "b", "binary-search", true, "Use this class as a comparator to binary search", false);
        addOption(options, "a", "binary-arg", true, "Argument to pass to binary search class", false);
        addOption(options, "r", "as-method-reader", true, "Use when reading from a queue generated using a MethodWriter", false);
        addOption(options, "g", "message-history", false, "Show message history (when using method reader)", false);
        addOption(options, "w", "wire-type", true, "Control output i.e. JSON", false);
        addOption(options, "s", "suppress-index", false, "Display index", false);
        addOption(options, "l", "single-line", false, "Squash each output message into a single line", false);
        addOption(options, "z", "use-local-timezone", false, "Print timestamps using the local timezone", false);
        addOption(options, "k", "reverse", false, "Read the queue in reverse", false);
        addOption(options, "h", "help-message", false, "Print this help and exit", false);
        addOption(options, "x", "max-results", true, "Limit the number of results to output", false);
        addOption(options, "cbl", "content-based-limiter", true, "Specify a content-based limiter", false);
        addOption(options, "cblArg", "content-based-limiter-argument", true, "Specify an argument for use by the content-based limiter", false);
        return options;
    }
}
