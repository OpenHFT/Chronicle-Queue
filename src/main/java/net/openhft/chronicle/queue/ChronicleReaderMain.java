/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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
 * Main class for reading and displaying records from a Chronicle Queue in text form.
 * Provides several command-line options to control behavior such as including/excluding records
 * based on regex, following a live queue, or displaying records in various formats.
 */
public class ChronicleReaderMain {

    /**
     * Entry point of the application. Initializes the {@link ChronicleReaderMain} instance and
     * passes command-line arguments for execution.
     *
     * @param args Command-line arguments
     */
    public static void main(@NotNull String[] args) {
        new ChronicleReaderMain().run(args);
    }

    /**
     * Adds an option to the provided {@link Options} object for command-line parsing.
     *
     * @param options     The options object to add the option to
     * @param opt         The short name of the option
     * @param argName     The name of the argument
     * @param hasArg      Whether the option takes an argument
     * @param description Description of the option
     * @param isRequired  Whether the option is required
     */
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

    /**
     * Runs the Chronicle Reader with the provided command-line arguments.
     * Configures the {@link ChronicleReader} and executes the reader.
     *
     * @param args Command-line arguments
     */
    protected void run(@NotNull String[] args) {
        final Options options = options();
        final CommandLine commandLine = parseCommandLine(args, options);

        final ChronicleReader chronicleReader = chronicleReader();

        configureReader(chronicleReader, commandLine);

        chronicleReader.execute();
    }

    /**
     * Creates and returns a new instance of {@link ChronicleReader}.
     *
     * @return A new instance of {@link ChronicleReader}
     */
    protected ChronicleReader chronicleReader() {
        return new ChronicleReader();
    }

    /**
     * Parses the command-line arguments using Apache Commons CLI.
     * If the help option is provided, prints the help message and exits.
     *
     * @param args    Command-line arguments
     * @param options Command-line options available
     * @return The parsed {@link CommandLine} object
     */
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

    /**
     * Prints help information and exits the application.
     *
     * @param options Command-line options
     * @param status  Exit status code
     */
    protected void printHelpAndExit(final Options options, int status) {
        printHelpAndExit(options, status, null);
    }

    /**
     * Prints help information along with an optional message and exits the application.
     *
     * @param options Command-line options
     * @param status  Exit status code
     * @param message Optional message to display before help
     */
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

    /**
     * Configures the {@link ChronicleReader} based on the command-line options.
     * Supports various options like regex filtering, tailing the queue, and more.
     *
     * @param chronicleReader The ChronicleReader instance to configure
     * @param commandLine     Parsed command-line options
     */
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
            chronicleReader.asMethodReader(r.equals("null") ? "" : r);
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
        if (commandLine.hasOption("named")) {
            chronicleReader.withTailerId(commandLine.getOptionValue("named"));
        }
    }

    /**
     * Configures the available command-line options for the {@link ChronicleReaderMain}.
     *
     * @return A configured {@link Options} object with all available options
     */
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
        addOption(options, "named", "named", true, "Named tailer ID", false);
        return options;
    }
}
