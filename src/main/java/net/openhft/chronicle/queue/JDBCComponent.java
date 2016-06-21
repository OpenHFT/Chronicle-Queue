package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.util.ThrowingSupplier;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

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
    public void executeQuery(String query, Object... args) {
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            for (int i = 0; i < args.length; i++)
                ps.setObject(i + 1, args[i]);
            ResultSet resultSet = ps.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            List<String> headings = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; i++)
                headings.add(metaData.getColumnName(i));

            List<List<Object>> rows = new ArrayList<>();
            while (resultSet.next()) {
                List<Object> row = new ArrayList<>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    row.add(resultSet.getObject(i));
                }
                rows.add(row);
            }
            result.queryResult(headings, rows, query, args);

        } catch (Throwable t) {
            result.queryThrown(t, query, args);
        }
    }
}
