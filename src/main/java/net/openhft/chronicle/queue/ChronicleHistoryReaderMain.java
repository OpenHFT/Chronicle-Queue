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

import net.openhft.chronicle.queue.reader.ChronicleHistoryReader;
import org.apache.commons.cli.*;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * Reads @see MessageHistory from a chronicle and outputs histograms for
 * <ul>
 * <li>latencies for each component that has processed a message</li>
 * <li>latencies between each component that has processed a message</li>
 * </ul>
 * @author Jerry Shea
 */
public class ChronicleHistoryReaderMain {

    public static void main(@NotNull String[] args) throws IOException {
        new ChronicleHistoryReaderMain().run(args);
    }

    protected void run(String[] args) throws IOException {
        final Options options = options();
        final CommandLine commandLine = parseCommandLine(args, options);

        final ChronicleHistoryReader chronicleHistoryReader = chronicleHistoryReader();
        setup(commandLine, chronicleHistoryReader);
        chronicleHistoryReader.execute();
    }

    protected void setup(CommandLine commandLine, ChronicleHistoryReader chronicleHistoryReader) throws IOException {
        chronicleHistoryReader.
                withMessageSink(System.out::println).
                withProgress(commandLine.hasOption('p')).
                withHistosByMethod(commandLine.hasOption('m')).
                withSummaryOutput(commandLine.hasOption('u')).
                withBasePath(Paths.get(commandLine.getOptionValue('d')));
        if (commandLine.hasOption('t'))
            chronicleHistoryReader.withTimeUnit(TimeUnit.valueOf(commandLine.getOptionValue('t')));
        if (commandLine.hasOption('i'))
            chronicleHistoryReader.withIgnore(Long.parseLong(commandLine.getOptionValue('i')));
        if (commandLine.hasOption('w'))
            chronicleHistoryReader.withMeasurementWindow(Long.parseLong(commandLine.getOptionValue('w')));
    }

    @NotNull
    protected ChronicleHistoryReader chronicleHistoryReader() {
        return new ChronicleHistoryReader();
    }

    protected CommandLine parseCommandLine(final @NotNull String[] args, final Options options) {
        final CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);

            if (commandLine.hasOption('h')) {
                new HelpFormatter().printHelp(ChronicleHistoryReaderMain.class.getSimpleName(), options);
                System.exit(0);
            }
        } catch (ParseException e) {
            printUsageAndExit(options);
        }

        return commandLine;
    }

    protected void printUsageAndExit(final Options options) {
        final PrintWriter writer = new PrintWriter(System.out);
        new HelpFormatter().printUsage(writer, 180,
                ChronicleHistoryReaderMain.class.getSimpleName(), options);
        writer.flush();
        System.exit(1);
    }

    @NotNull
    protected Options options() {
        final Options options = new Options();
        ChronicleReaderMain.addOption(options, "d", "directory", true, "Directory containing chronicle queue files", true);
        ChronicleReaderMain.addOption(options, "h", "help-message", false, "Print this help and exit", false);
        ChronicleReaderMain.addOption(options, "t", "time unit", true, "Time unit. Default nanos", false);
        ChronicleReaderMain.addOption(options, "i", "ignore", true, "How many to ignore from start", false);
        ChronicleReaderMain.addOption(options, "w", "window", true, "Window duration in time unit", false);
        options.addOption(new Option("p", false, "Show progress"));
        options.addOption(new Option("m", false, "By method"));
        options.addOption(new Option("u", false, "Summary output"));
        return options;
    }
}