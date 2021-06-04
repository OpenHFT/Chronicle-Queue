package net.openhft.chronicle.queue.internal.writer;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Marshallable;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class ChronicleWriter {
    private Path basePath;
    private String methodName;
    private List<String> files;

    public void execute() throws IOException {
        try (final ChronicleQueue queue = createQueue()) {
            final ExcerptAppender appender = queue.acquireAppender();
            for (final String file : files) {
                final Object payload = Marshallable.fromFile(Object.class, file);
                try (final DocumentContext dc = appender.writingDocument()) {
                    dc.wire().write(methodName).object(payload);
                }
            }
        }
    }

    private ChronicleQueue createQueue() {
        return ChronicleQueue.singleBuilder(this.basePath).build();
    }

    public ChronicleWriter withBasePath(final Path path) {
        this.basePath = path;
        return this;
    }

    public void asMethodWriter(String interfaceName) {
        try {
            // just load the class, we don't need to refer to it
            Class.forName(interfaceName);
        } catch (ClassNotFoundException e) {
            throw Jvm.rethrow(e);
        }
    }

    public ChronicleWriter withMethodName(String methodName) {
        this.methodName = methodName;
        return this;
    }

    public ChronicleWriter withFiles(List<String> files) {
        this.files = files;
        return this;
    }
}
