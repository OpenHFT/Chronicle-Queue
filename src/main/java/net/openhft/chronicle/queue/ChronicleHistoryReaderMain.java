/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.reader.ChronicleHistoryReader;
import net.openhft.chronicle.wire.MessageHistory;
import org.apache.commons.cli.*;
import org.jetbrains.annotations.NotNull;

import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * The main class for reading {@link MessageHistory} from a Chronicle queue
 * and generating histograms for:
 * <ul>
 *     <li>Latencies for each component that has processed a message</li>
 *     <li>Latencies between each component that has processed a message</li>
 * </ul>
 * This class provides options to configure the reader via command line arguments and outputs the results
 * to the console.
 */
public class ChronicleHistoryReaderMain {

    /**
     * Entry point of the application.
     * Initializes the {@link ChronicleHistoryReaderMain} and passes command-line arguments.
     *
     * @param args Command-line arguments
     */
    public static void main(@NotNull String[] args) {
        new ChronicleHistoryReaderMain().run(args);
    }

    /**
     * Runs the ChronicleHistoryReader setup and execution.
     * Parses command-line options and configures the {@link ChronicleHistoryReader}.
     *
     * @param args Command-line arguments
     */
    protected void run(String[] args) {
        final Options options = options();
        final CommandLine commandLine = parseCommandLine(args, options);

        try (final ChronicleHistoryReader chronicleHistoryReader = chronicleHistoryReader()) {
            setup(commandLine, chronicleHistoryReader);
            chronicleHistoryReader.execute();
        }
    }

    /**
     * Configures the {@link ChronicleHistoryReader} based on the command-line options.
     *
     * @param commandLine Parsed command-line options
     * @param chronicleHistoryReader The history reader to configure
     */
    protected void setup(@NotNull final CommandLine commandLine, @NotNull final ChronicleHistoryReader chronicleHistoryReader) {
        chronicleHistoryReader.
                withMessageSink(System.out::println).
                withProgress(commandLine.hasOption('p')).
                withHistosByMethod(commandLine.hasOption('m')).
                withBasePath(Paths.get(commandLine.getOptionValue('d')));
        if (commandLine.hasOption('t'))
            chronicleHistoryReader.withTimeUnit(TimeUnit.valueOf(commandLine.getOptionValue('t')));
        if (commandLine.hasOption('i'))
            chronicleHistoryReader.withIgnore(Long.parseLong(commandLine.getOptionValue('i')));
        if (commandLine.hasOption('w'))
            chronicleHistoryReader.withMeasurementWindow(Long.parseLong(commandLine.getOptionValue('w')));
        if (commandLine.hasOption('u'))
            chronicleHistoryReader.withSummaryOutput(Integer.parseInt(commandLine.getOptionValue('u')));
    }

    /**
     * Initializes a new instance of {@link ChronicleHistoryReader}.
     *
     * @return A new {@link ChronicleHistoryReader} instance
     */
    @NotNull
    protected ChronicleHistoryReader chronicleHistoryReader() {
        return new ChronicleHistoryReader();
    }

    /**
     * Parses command-line arguments using Apache Commons CLI.
     * If help is requested, it prints the help message and exits.
     *
     * @param args    Command-line arguments
     * @param options Available command-line options
     * @return Parsed {@link CommandLine} object
     */
    protected CommandLine parseCommandLine(@NotNull final String[] args, final Options options) {
        final CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);

            if (commandLine.hasOption('h')) {
                printHelpAndExit(options, 0);
            }
        } catch (ParseException e) {
            // If parsing fails, print help with an error message and exit
            String cmdLine = Arrays.toString(args);
            printHelpAndExit(options, "[-h]".equals(cmdLine) ? 0 : 1, e.getMessage());
        }

        return commandLine;
    }

    /**
     * Prints help and exits the program.
     *
     * @param options Command-line options
     * @param status  Exit status
     */
    protected void printHelpAndExit(final Options options, int status) {
        printHelpAndExit(options, status, null);
    }

    /**
     * Prints help and exits the program, optionally with a message.
     *
     * @param options Command-line options
     * @param status  Exit status
     * @param message Optional message to print before help
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
     * Configures command-line options for the ChronicleHistoryReaderMain.
     *
     * @return Configured {@link Options} object
     */
    @NotNull
    protected Options options() {
        final Options options = new Options();
        ChronicleReaderMain.addOption(options, "d", "directory", true, "Directory containing chronicle queue files", true);
        ChronicleReaderMain.addOption(options, "h", "help-message", false, "Print this help and exit", false);
        ChronicleReaderMain.addOption(options, "t", "time unit", true, "Time unit. Default nanos", false);
        ChronicleReaderMain.addOption(options, "i", "ignore", true, "How many items to ignore from start", false);
        ChronicleReaderMain.addOption(options, "w", "window", true, "Window duration in time unit. Instead of one output at the end, will output every window period", false);
        ChronicleReaderMain.addOption(options, "u", "histo offset", true, "Summary output. Instead of histograms, will show one value only, in CSV format. Set this to 0 for 50th, 1 for 90th etc., -1 for worst", false);
        options.addOption(new Option("p", false, "Show progress"));
        options.addOption(new Option("m", false, "By method"));
        return options;
    }
}
