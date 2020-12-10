/*
 * Copyright 2016-2020 chronicle.software
 *
 * https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue.internal.jdbc;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.util.ThrowingSupplier;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.JDBCResult;
import net.openhft.chronicle.queue.JDBCServiceProvider;
import net.openhft.chronicle.queue.JDBCStatement;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.threads.Pauser;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class InternalJDBCService extends AbstractCloseable implements Closeable, JDBCServiceProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(InternalJDBCService.class);
    @NotNull
    private final ChronicleQueue in;
    private final ChronicleQueue out;
    private final ThrowingSupplier<Connection, SQLException> connectionSupplier;

    public InternalJDBCService(@NotNull ChronicleQueue in, ChronicleQueue out, ThrowingSupplier<Connection, SQLException> connectionSupplier) {
        this.in = in;
        this.out = out;
        this.connectionSupplier = connectionSupplier;

        ExecutorService service = Executors.newSingleThreadExecutor(
                new NamedThreadFactory(in.file().getName() + "-JDBCService", true));
        service.execute(this::runLoop);
        service.shutdown(); // stop when the task exits.
    }

    void runLoop() {
        try {
            JDBCResult result = out.acquireAppender()
                    .methodWriterBuilder(JDBCResult.class)
                    .get();
            JDBCStatement js = JDBCStatement.create(connectionSupplier, result);
            MethodReader reader = in.createTailer().afterLastWritten(out).methodReader(js);
            Pauser pauser = Pauser.millis(1, 10);
            while (!isClosed()) {
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
    protected void performClose() {
    }

    @NotNull
    public JDBCStatement createWriter() {
        return in.acquireAppender()
                .methodWriterBuilder(JDBCStatement.class)
                .get();
    }

    @NotNull
    public MethodReader createReader(JDBCResult result) {
        return out.createTailer().methodReader(result);
    }
}
