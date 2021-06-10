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

public class ChronicleWriter {
    private Path basePath;
    private String methodName;
    private List<String> files;
    private Class<?> writeTo;

    public void execute() throws IOException {
        try (final ChronicleQueue queue = ChronicleQueue.singleBuilder(this.basePath).build()) {
            final ExcerptAppender appender = queue.acquireAppender();

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
     * Chronicle queue base path
     * @param path path of queue to write to
     * @return this
     */
    public ChronicleWriter withBasePath(final Path path) {
        this.basePath = path;
        return this;
    }

    /**
     * Interface class to use to write via
     * @param interfaceName interface
     * @return this
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
     * Specify method name to write each message out as
     * @param methodName method name
     * @return this
     */
    public ChronicleWriter withMethodName(String methodName) {
        this.methodName = methodName;
        return this;
    }

    /**
     * List of files to read and, for each, write out a message preceded by {@link #methodName}
     * @param files files
     * @return this
     */
    public ChronicleWriter withFiles(List<String> files) {
        this.files = files;
        return this;
    }
}
