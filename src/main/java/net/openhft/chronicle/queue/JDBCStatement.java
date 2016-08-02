package net.openhft.chronicle.queue;

/**
 * Created by peter on 06/04/16.
 */
public interface JDBCStatement {
    void executeQuery(String query, Object... args);

    void executeUpdate(String query, Object... args);
}
