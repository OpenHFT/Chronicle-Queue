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

import net.openhft.chronicle.queue.reader.ChronicleHistoryReader;
import org.apache.commons.cli.*;
import org.jetbrains.annotations.NotNull;

import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * Reads @see MessageHistory from a chronicle and outputs histograms for
 * <ul>
 * <li>latencies for each component that has processed a message</li>
 * <li>latencies between each component that has processed a message</li>
 * </ul>
 *
 * @author Jerry Shea
 */
public class ChronicleHistoryReaderMain {

    public static void main(@NotNull String[] args) {
        new ChronicleHistoryReaderMain().run(args);
    }

    protected void run(String[] args) {
        final Options options = options();
        final CommandLine commandLine = parseCommandLine(args, options);

        final ChronicleHistoryReader chronicleHistoryReader = chronicleHistoryReader();
        setup(commandLine, chronicleHistoryReader);
        chronicleHistoryReader.execute();
    }

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

    @NotNull
    protected ChronicleHistoryReader chronicleHistoryReader() {
        return new ChronicleHistoryReader();
    }

    protected CommandLine parseCommandLine(@NotNull final String[] args, final Options options) {
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
