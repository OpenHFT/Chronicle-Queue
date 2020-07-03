/*
 * Copyright 2016 higherfrequencytrading.com
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

package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

class CountingJDBCResult implements JDBCResult {
    private final AtomicLong queries;
    private final AtomicLong updates;

    public CountingJDBCResult(AtomicLong queries, AtomicLong updates) {
        this.queries = queries;
        this.updates = updates;
    }

    @Override
    public void queryResult(List<String> columns, @NotNull List<List<Object>> rows, String query, Object... args) {
//        System.out.println("query " + query + " returned " + columns);
/*
        for (List<Object> row : rows) {
            System.out.println("\t" + row);
        }
*/
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
