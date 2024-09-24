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

import org.jetbrains.annotations.NotNull;

/**
 * The entry point for writing records to a Chronicle Queue.
 * This class delegates the writing task to the internal {@link net.openhft.chronicle.queue.internal.writer.ChronicleWriterMain}.
 */
public class ChronicleWriterMain {

    /**
     * Main method for executing the ChronicleWriterMain.
     * It delegates to the internal writer implementation to run the application with the given arguments.
     *
     * @param args Command-line arguments for the writer
     * @throws Exception if an error occurs during execution
     */
    public static void main(@NotNull String[] args) throws Exception {
        // Delegate the task to the internal ChronicleWriterMain to handle the actual writing process
        new net.openhft.chronicle.queue.internal.writer.ChronicleWriterMain().run(args);
    }
}
