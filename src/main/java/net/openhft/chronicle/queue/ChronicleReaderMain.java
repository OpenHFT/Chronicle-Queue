/*
 * Copyright 2014 Higher Frequency Trading
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
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
        final Option option = new Option(opt, hasArg, description); // Create option with argument
        option.setArgName(argName);
        option.setRequired(isRequired); // Mark as required or not
        options.addOption(option); // Add option to options object
    }

    /**
     * Runs the Chronicle Reader with the provided command-line arguments.
     * Configures the {@link ChronicleReader} and executes the reader.
     *
     * @param args Command-line arguments
     */
    protected void run(@NotNull String[] args) {
        final Options options = options(); // Initialize command-line options
        final CommandLine commandLine = parseCommandLine(args, options); // Parse command-line options

        final ChronicleReader chronicleReader = chronicleReader(); // Create ChronicleReader instance

        configureReader(chronicleReader, commandLine); // Configure the reader based on options

        chronicleReader.execute(); // Execute the reader to display records
    }

    /**
     * Creates and returns a new instance of {@link ChronicleReader}.
     *
     * @return A new instance of {@link ChronicleReader}
     */
    protected ChronicleReader chronicleReader() {
        return new ChronicleReader(); // Create and return ChronicleReader instance
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
        final CommandLineParser parser = new DefaultParser(); // Command-line parser
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args); // Parse arguments

            // Print help if 'h' option is provided
            if (commandLine.hasOption('h')) {
                printHelpAndExit(options, 0);
            }
        } catch (ParseException e) {
            // On parsing error, print help with an error message
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
                180, // Line width for formatted help output
                this.getClass().getSimpleName(),
                message,
                options,
                HelpFormatter.DEFAULT_LEFT_PAD,
                HelpFormatter.DEFAULT_DESC_PAD,
                null,
                true
        );
        writer.flush(); // Ensure all help is printed
        System.exit(status); // Exit with the provided status
    }

    /**
     * Configures the {@link ChronicleReader} based on the command-line options.
     * Supports various options like regex filtering, tailing the queue, and more.
     *
     * @param chronicleReader The ChronicleReader instance to configure
     * @param commandLine     Parsed command-line options
     */
    protected void configureReader(final ChronicleReader chronicleReader, final CommandLine commandLine) {
        // Set up message sink; squash output to single line if 'l' option is provided
        final Consumer<String> messageSink = commandLine.hasOption('l') ?
                s -> System.out.println(s.replaceAll("\n", "")) :
                System.out::println;
        chronicleReader
                .withMessageSink(messageSink) // Configure the message sink
                .withBasePath(Paths.get(commandLine.getOptionValue('d'))); // Set base path for chronicle queue files

        // Apply various optional configurations based on command-line options
        if (commandLine.hasOption('i')) {
            stream(commandLine.getOptionValues('i')).forEach(chronicleReader::withInclusionRegex);
        }
        if (commandLine.hasOption('e')) {
            stream(commandLine.getOptionValues('e')).forEach(chronicleReader::withExclusionRegex);
        }
        if (commandLine.hasOption('f')) {
            chronicleReader.tail(); // Enable tail mode if 'f' option is provided
        }
        if (commandLine.hasOption('m')) {
            chronicleReader.historyRecords(Long.parseLong(commandLine.getOptionValue('m'))); // Limit history records
        }
        if (commandLine.hasOption('n')) {
            chronicleReader.withStartIndex(Long.decode(commandLine.getOptionValue('n'))); // Set start index
        }
        if (commandLine.hasOption('r')) {
            final String r = commandLine.getOptionValue('r');
            chronicleReader.asMethodReader(r.equals("null") ? "" : r); // Configure as method reader
            chronicleReader.showMessageHistory(commandLine.hasOption('g')); // Show message history if 'g' is present
        }
        if (commandLine.hasOption('w')) {
            chronicleReader.withWireType(WireType.valueOf(commandLine.getOptionValue('w'))); // Set wire type
        }
        if (commandLine.hasOption('s')) {
            chronicleReader.suppressDisplayIndex(); // Suppress index display if 's' is present
        }
        if (commandLine.hasOption('z')) {
            System.setProperty(AbstractTimestampLongConverter.TIMESTAMP_LONG_CONVERTERS_ZONE_ID_SYSTEM_PROPERTY,
                    ZoneId.systemDefault().toString()); // Use local timezone if 'z' is present
        }
        if (commandLine.hasOption('a')) {
            chronicleReader.withArg(commandLine.getOptionValue('a')); // Pass argument to binary search if 'a' is present
        }
        if (commandLine.hasOption('b')) {
            chronicleReader.withBinarySearch(commandLine.getOptionValue('b')); // Configure binary search
        }
        if (commandLine.hasOption('k')) {
            chronicleReader.inReverseOrder(); // Read the queue in reverse if 'k' is present
        }
        if (commandLine.hasOption('x')) {
            chronicleReader.withMatchLimit(Long.parseLong(commandLine.getOptionValue('x'))); // Limit match results
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
            chronicleReader.withLimiterArg(commandLine.getOptionValue("cblArg")); // Set content-based limiter argument
        }
        if (commandLine.hasOption("named")) {
            chronicleReader.withTailerId(commandLine.getOptionValue("named")); // Set named tailer ID
        }
    }

    /**
     * Configures the available command-line options for the {@link ChronicleReaderMain}.
     *
     * @return A configured {@link Options} object with all available options
     */
    @NotNull
    protected Options options() {
        final Options options = new Options(); // Create new Options object

        // Add various command-line options
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

        return options; // Return configured options
    }
}
