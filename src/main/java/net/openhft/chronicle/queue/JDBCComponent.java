package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.util.ThrowingSupplier;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.sql.*;
import java.util.Iterator;

/**
 * Created by peter on 12/04/16.
 */
public class JDBCComponent implements JDBCStatement {
    private final Connection connection;
    private final JDBCResult result;

    public JDBCComponent(ThrowingSupplier<Connection, SQLException> connectionSupplier, JDBCResult result) throws SQLException {
        connection = connectionSupplier.get();
        this.result = result;
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
}
