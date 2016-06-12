package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.util.ThrowingSupplier;
import net.openhft.chronicle.threads.LongPauser;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.MethodReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by peter on 06/04/16.
 */
public class JDBCService implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCService.class);
    private final ChronicleQueue in;
    private final ChronicleQueue out;
    private final ExecutorService service;
    private final ThrowingSupplier<Connection, SQLException> connectionSupplier;
    private volatile boolean closed = false;

    public JDBCService(ChronicleQueue in, ChronicleQueue out, ThrowingSupplier<Connection, SQLException> connectionSupplier) throws SQLException {
        this.in = in;
        this.out = out;
        this.connectionSupplier = connectionSupplier;

        service = Executors.newSingleThreadExecutor(
                new NamedThreadFactory(in.file().getName() + "-JDBCService", true));
        service.execute(this::runLoop);
        service.shutdown(); // stop when the task exits.
    }

    void runLoop() {
        try {
            JDBCResult result = out.createAppender()
                    .methodWriterBuilder(JDBCResult.class)
                    .recordHistory(true)
                    .get();
            JDBCComponent js = new JDBCComponent(connectionSupplier, result);
            MethodReader reader = in.createTailer().afterLastWritten(out).methodReader(js);
            Pauser pauser = new LongPauser(50, 200, 1, 10, TimeUnit.MILLISECONDS);
            while (!closed) {
                if (reader.readOne())
                    pauser.reset();
                else
                    pauser.pause();
            }
        } catch (Throwable t) {
            LOGGER.warn("Run loop exited", t);
        }
    }

    @Override
    public void close() {
        closed = true;
    }

    public JDBCStatement createWriter() {
        return in.createAppender()
                .methodWriterBuilder(JDBCStatement.class)
                .recordHistory(true)
                .get();
    }

    public MethodReader createReader(JDBCResult result) {
        return out.createTailer().methodReader(result);
    }
}
