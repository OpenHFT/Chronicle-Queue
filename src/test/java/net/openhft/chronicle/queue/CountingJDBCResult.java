package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Peter on 18/06/2016.
 */
class CountingJDBCResult implements JDBCResult {
    private final AtomicLong queries;
    private final AtomicLong updates;

    public CountingJDBCResult(AtomicLong queries, AtomicLong updates) {
        this.queries = queries;
        this.updates = updates;
    }

    @Override
    public void queryResult(List<String> columns, List<List<Object>> rows, String query, Object... args) {
        System.out.println("query " + query + " returned " + columns);
        for (List<Object> row : rows) {
            System.out.println("\t" + row);
        }
        queries.incrementAndGet();
    }

    @Override
    public void queryThrown(Throwable t, String query, Object... args) {
        throw Jvm.rethrow(t);
    }

    @Override
    public void updateResult(long count, String update, Object... args) {
        updates.incrementAndGet();
    }

    @Override
    public void updateThrown(Throwable t, String update, Object... args) {
        throw Jvm.rethrow(t);
    }
}
