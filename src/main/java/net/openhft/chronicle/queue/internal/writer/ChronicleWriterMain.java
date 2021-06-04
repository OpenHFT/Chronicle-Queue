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

package net.openhft.chronicle.queue.internal.writer;

import org.apache.commons.cli.*;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.List;

import static net.openhft.chronicle.queue.ChronicleReaderMain.addOption;

public class ChronicleWriterMain {

    public void run(@NotNull String[] args) throws IOException {
        final Options options = options();
        final CommandLine commandLine = parseCommandLine(args, options);

        final ChronicleWriter writer = new ChronicleWriter();

        configure(writer, commandLine);

        writer.execute();
    }

    private CommandLine parseCommandLine(final @NotNull String[] args, final Options options) {
        final CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);

            if (commandLine.hasOption('h')) {
                printHelpAndExit(options, 0, null);
            }

            if (commandLine.getArgList().size() < 2) {
                printHelpAndExit(options, 1, "Need methodName and files...");
            }
        } catch (ParseException e) {
            printHelpAndExit(options, 1, e.getMessage());
        }

        return commandLine;
    }

    private void printHelpAndExit(final Options options, int status, String message) {
        final PrintWriter writer = new PrintWriter(System.out);
        new HelpFormatter().printHelp(
                writer,
                180,
                this.getClass().getSimpleName() + " methodName files..",
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

    private void configure(final ChronicleWriter writer, final CommandLine commandLine) {
        writer.withBasePath(Paths.get(commandLine.getOptionValue('d')));

        if (commandLine.hasOption('r')) {
            final String r = commandLine.getOptionValue('r');
            writer.asMethodWriter(r.equals("null") ? null : r);
        }

        final List<String> args = commandLine.getArgList();
        writer.withMethodName(args.get(0));

        writer.withFiles(args.subList(1, args.size()));
    }

    @NotNull
    private Options options() {
        final Options options = new Options();

        addOption(options, "d", "directory", true, "Directory containing chronicle queue files", true);
        addOption(options, "i", "interface", true, "Interface to load up", false);
        return options;
    }
}
