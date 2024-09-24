/*
 * Copyright 2016-2022 chronicle.software
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

package net.openhft.chronicle.queue.internal.writer;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WriteMarshallable;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * The {@code ChronicleWriter} class is responsible for writing objects to a Chronicle Queue.
 * <p>
 * It reads data from a list of files and writes the contents to the queue, optionally using a method writer
 * if an interface class is provided.
 * </p>
 */
public class ChronicleWriter {
    private Path basePath;        // The base path of the Chronicle Queue
    private String methodName;    // The method name used to write each message
    private List<String> files;   // List of files to read from and write to the queue
    private Class<?> writeTo;     // The interface class for method writing

    /**
     * Executes the process of reading from files and writing their contents to the Chronicle Queue.
     *
     * @throws IOException if an error occurs during file reading or queue writing
     */
    public void execute() throws IOException {
        try (final ChronicleQueue queue = ChronicleQueue.singleBuilder(this.basePath).build();
             final ExcerptAppender appender = queue.createAppender()) {

            for (final String file : files) {
                final Object payload = Marshallable.fromFile(Object.class, file); // Load the file into a payload object
                try (final DocumentContext dc = appender.writingDocument()) {
                    if (writeTo != null)
                        dc.wire().write(methodName).marshallable((WriteMarshallable) payload); // Use method writer if interface is provided
                    else
                        dc.wire().write(methodName).object(payload); // Write as a generic object if no method writer
                }
            }
        }
    }

    /**
     * Sets the base path of the Chronicle Queue to write to.
     *
     * @param path The path of the Chronicle Queue
     * @return This {@code ChronicleWriter} instance for method chaining
     */
    public ChronicleWriter withBasePath(final Path path) {
        this.basePath = path;
        return this;
    }

    /**
     * Sets the interface class to use for writing through method calls.
     * <p>
     * This method allows writing through a method writer by specifying the name of an interface class.
     * </p>
     *
     * @param interfaceName The fully qualified name of the interface class
     * @return This {@code ChronicleWriter} instance for method chaining
     */
    public ChronicleWriter asMethodWriter(String interfaceName) {
        try {
            this.writeTo = Class.forName(interfaceName); // Load the interface class
        } catch (ClassNotFoundException e) {
            throw Jvm.rethrow(e); // Handle class loading error
        }
        return this;
    }

    /**
     * Sets the method name to use when writing each message.
     *
     * @param methodName The method name
     * @return This {@code ChronicleWriter} instance for method chaining
     */
    public ChronicleWriter withMethodName(String methodName) {
        this.methodName = methodName;
        return this;
    }

    /**
     * Sets the list of files to read from and write as messages to the queue.
     *
     * @param files The list of file paths
     * @return This {@code ChronicleWriter} instance for method chaining
     */
    public ChronicleWriter withFiles(List<String> files) {
        this.files = files;
        return this;
    }
}
