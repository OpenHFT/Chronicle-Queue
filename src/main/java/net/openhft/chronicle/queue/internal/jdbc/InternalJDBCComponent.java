/*
 * Copyright 2016-2020 chronicle.software
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.queue.internal.jdbc;

import net.openhft.chronicle.core.util.ThrowingSupplier;
import net.openhft.chronicle.queue.JDBCResult;
import net.openhft.chronicle.queue.JDBCStatement;
import org.jetbrains.annotations.NotNull;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public final class InternalJDBCComponent implements JDBCStatement {
    @NotNull
    private final Connection connection;
    private final JDBCResult result;

    public InternalJDBCComponent(@NotNull ThrowingSupplier<Connection, SQLException> connectionSupplier, JDBCResult result) throws SQLException {
        connection = connectionSupplier.get();
        this.result = result;
    }

    @Override
    public void executeUpdate(String query, @NotNull Object... args) {
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
    public void executeQuery(String query, @NotNull Object... args) {
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
