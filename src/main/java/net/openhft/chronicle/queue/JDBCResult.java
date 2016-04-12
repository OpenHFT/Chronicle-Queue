package net.openhft.chronicle.queue;

import net.openhft.chronicle.wire.Marshallable;

import java.util.Iterator;

/**
 * Created by peter on 06/04/16.
 */
public interface JDBCResult {
    void queryResult(Iterator<Marshallable> marshallableList, String query, Object... args);

    void queryThrown(Throwable t, String query, Object... args);

    void updateResult(long count, String update, Object... args);

    void updateThrown(Throwable t, String update, Object... args);
}
