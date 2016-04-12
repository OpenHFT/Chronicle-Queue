package net.openhft.chronicle.queue;

import net.openhft.chronicle.wire.Marshallable;

/**
 * Created by peter on 06/04/16.
 */
public interface JDBCStatement {
    void executeQuery(String query, Class<? extends Marshallable> resultType, Object... args);

    void executeUpdate(String query, Object... args);
}
