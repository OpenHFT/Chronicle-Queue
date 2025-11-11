/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.writer;

import org.apache.commons.cli.*;
import org.jetbrains.annotations.NotNull;

import java.io.PrintWriter;
import java.nio.file.Paths;

import static net.openhft.chronicle.queue.ChronicleReaderMain.addOption;

/**
 * {@code ChronicleWriterMain} is the main class responsible for configuring and running the {@link ChronicleWriter}
 * from the command line. It processes command-line arguments to determine how data should be written to the Chronicle Queue.
 */
public class ChronicleWriterMain {

    private static final int HELP_OUTPUT_LINE_WIDTH = 180;

    /**
     * Runs the ChronicleWriter based on the provided command-line arguments.
     *
     * @param args Command-line arguments
     * @throws Exception If an error occurs during the writing process
     */
    public void run(@NotNull String[] args) throws Exception {
        final Options options = options();
        final CommandLine commandLine = parseCommandLine(args, options);

        final ChronicleWriter writer = new ChronicleWriter();

        configure(writer, commandLine);

        writer.execute();
    }

    /**
     * Parses the command-line arguments using Apache Commons CLI.
     * <p>If there are issues with parsing or required arguments are missing, it prints help and exits the program.
     *
     * @param args    Command-line arguments
     * @param options The defined options for command-line parsing
     * @return The parsed {@link CommandLine} object
     */
    private CommandLine parseCommandLine(final @NotNull String[] args, final Options options) {
        final CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);

            if (commandLine.hasOption('h')) {
                printHelpAndExit(options, 0, null);
            }

            if (commandLine.getArgList().isEmpty()) {
                printHelpAndExit(options, 1, "Need files...");
            }
        } catch (ParseException e) {
            printHelpAndExit(options, 1, e.getMessage());
        }

        return commandLine;
    }

    /**
     * Prints the help message and exits the application.
     *
     * @param options Command-line options
     * @param status  Exit status code
     * @param message Optional message to display before the help
     */
    private void printHelpAndExit(final Options options, int status, String message) {
        final PrintWriter writer = new PrintWriter(System.out);
        new HelpFormatter().printHelp(
                writer,
                180,
                this.getClass().getSimpleName() + " files..",
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
     * Configures the {@link ChronicleWriter} based on the parsed command-line options.
     *
     * @param writer       The {@link ChronicleWriter} instance to configure
     * @param commandLine  The parsed command-line options
     */
    private void configure(final ChronicleWriter writer, final CommandLine commandLine) {
        writer.withBasePath(Paths.get(commandLine.getOptionValue('d')));
        writer.withMethodName(commandLine.getOptionValue('m'));

        if (commandLine.hasOption('i')) {
            final String r = commandLine.getOptionValue('i');
            writer.asMethodWriter(r.equals("null") ? null : r);
        }

        writer.withFiles(commandLine.getArgList());
    }

    /**
     * Defines the available command-line options for configuring the {@link ChronicleWriter}.
     *
     * @return A configured {@link Options} object with all available options
     */
    @NotNull
    private Options options() {
        final Options options = new Options();

        addOption(options, "m", "method", true, "Method name", true);
        addOption(options, "d", "directory", true, "Directory containing chronicle queue to write to", true);
        addOption(options, "i", "interface", true, "Interface to write via", false);
        return options;
    }
}
