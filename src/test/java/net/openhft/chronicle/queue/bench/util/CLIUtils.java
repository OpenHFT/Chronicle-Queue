package net.openhft.chronicle.queue.bench.util;

import org.apache.commons.cli.*;
import org.jetbrains.annotations.NotNull;

import java.io.PrintWriter;

public class CLIUtils {
    public static int getIntOption(CommandLine commandLine, char r, int defaultValue) {
        return Integer.parseInt(commandLine.getOptionValue(r, Integer.toString(defaultValue)));
    }

    public static CommandLine parseCommandLine(String appName, @NotNull final String[] args, final Options options) {
        final CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);

            if (commandLine.hasOption('h')) {
                printHelpAndExit(options, appName);
            }
        } catch (ParseException e) {
            printHelpAndExit(options, 1, e.getMessage(), appName);
        }

        return commandLine;
    }

    public static void printHelpAndExit(final Options options, String simpleName) {
        printHelpAndExit(options, 0, null, simpleName);
    }

    public static void printHelpAndExit(final Options options, int status, String message, String simpleName) {
        final PrintWriter writer = new PrintWriter(System.out);
        new HelpFormatter().printHelp(
                writer,
                180,
                simpleName,
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

    @NotNull
    public static Options createOptions() {
        Options options = new Options();
        addOption(options, "h", "help", false, "Help", false);
        addOption(options, "f", "result", false, "Write results to result.csv", false);
        addOption(options, "j", "jitter", false, "Record OS jitter", false);
        addOption(options, "t", "throughput", true, "Throughput", false);
        addOption(options, "i", "iterations", true, "Iterations", false);
        addOption(options, "r", "runs", true, "Number of runs", false);
        return options;
    }
}
