package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.util.ThrowingSupplier;
import net.openhft.chronicle.threads.LongPauser;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by peter on 06/04/16.
 */
public class JDBCService implements Runnable, Closeable, JDBCStatement {
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCService.class);
    private final ChronicleQueue in;
    private final ChronicleQueue out;
    private final Connection connection;
    private final ExecutorService service;
    private final JDBCStatement statement;
    private volatile boolean closed = false;

    public JDBCService(ChronicleQueue in, ChronicleQueue out, ThrowingSupplier<Connection, SQLException> connectionSupplier) throws SQLException {
        this.in = in;
        this.out = out;
        this.connection = connectionSupplier.get();
        String name = in.file().getName();
        service = Executors.newSingleThreadExecutor(new NamedThreadFactory(name + "-JDBCService", true));
        service.execute(this);
        statement = in.createAppender().methodWriterBuilder(JDBCStatement.class).recordHistory(true).get();
    }

    @Override
    public void executeQuery(String query, Class<? extends Marshallable> resultType, Object... args) {
        statement.executeQuery(query, resultType, args);
    }

    @Override
    public void executeUpdate(String query, Object... args) {
        statement.executeUpdate(query, args);
    }

    @Override
    public void run() {
        try {
            JDBCResult result = out.createAppender().methodWriterBuilder(JDBCResult.class).recordHistory(true).get();
            JSImpl js = new JSImpl(result);
            MethodReader reader = in.createTailer().afterLastWritten(out).methodReader(js);
            Pauser pauser = new LongPauser(50, 200, 1, 10, TimeUnit.MILLISECONDS);
            while (!closed) {
                if (reader.readOne())
                    pauser.reset();
                else
                    pauser.pause();
            }
        } catch (Throwable t) {
            LOGGER.error("Run loop exited", t);
        }
    }

    @Override
    public void close() {
        closed = true;
    }

    class JSImpl implements JDBCStatement {
        private final JDBCResult result;

        public JSImpl(JDBCResult result) {
            this.result = result;
        }

        @Override
        public void executeQuery(String query, Class<? extends Marshallable> resultType, Object... args) {
            try (PreparedStatement ps = connection.prepareStatement(query)) {
                for (int i = 0; i < args.length; i++)
                    ps.setObject(i + 1, args[i]);
                ResultSet resultSet = ps.executeQuery();
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                result.queryResult(new Iterator<Marshallable>() {
                    @Override
                    public boolean hasNext() {
                        try {
                            return resultSet.next();
                        } catch (SQLException e) {
                            throw Jvm.rethrow(e);
                        }
                    }

                    @Override
                    public Marshallable next() {
                        return new Marshallable() {
                            @Override
                            public void writeMarshallable(@NotNull WireOut wire) {
                                try {
                                    for (int i = 1; i <= columnCount; i++) {
                                        wire.writeEventName(metaData.getCatalogName(i))
                                                .object(resultSet.getObject(i));
                                    }
                                } catch (SQLException e) {
                                    throw Jvm.rethrow(e);
                                }
                            }
                        };
                    }
                }, query, args);
            } catch (Throwable t) {
                result.queryThrown(t, query, args);
            }
        }

        @Override
        public void executeUpdate(String query, Object... args) {
            try (PreparedStatement ps = connection.prepareStatement(query)) {
                for (int i = 0; i < args.length; i++)
                    ps.setObject(i + 1, args[i]);
                int count = ps.executeUpdate();
                // record the count.
                result.updateResult(count, query, args);
            } catch (Throwable t) {
                result.updateThrown(t, query, args);
            }
        }
    }
}
