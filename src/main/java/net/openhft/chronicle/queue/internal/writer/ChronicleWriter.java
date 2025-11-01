/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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
 *
 */
public class ChronicleWriter {
    private Path basePath;
    private String methodName;
    private List<String> files;
    private Class<?> writeTo;

    /**
     * Executes the process of reading from files and writing their contents to the Chronicle Queue.
     *
     * @throws IOException if an error occurs during file reading or queue writing
     */
    public void execute() throws IOException {
        try (final ChronicleQueue queue = ChronicleQueue.singleBuilder(this.basePath).build();
             final ExcerptAppender appender = queue.createAppender()) {

            for (final String file : files) {
                final Object payload = Marshallable.fromFile(Object.class, file);
                try (final DocumentContext dc = appender.writingDocument()) {
                    if (writeTo != null)
                        dc.wire().write(methodName).marshallable((WriteMarshallable) payload);
                    else
                        dc.wire().write(methodName).object(payload);
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
     *
     *
     * @param interfaceName The fully qualified name of the interface class
     * @return This {@code ChronicleWriter} instance for method chaining
     */
    public ChronicleWriter asMethodWriter(String interfaceName) {
        try {
            this.writeTo = Class.forName(interfaceName);
        } catch (ClassNotFoundException e) {
            throw Jvm.rethrow(e);
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
