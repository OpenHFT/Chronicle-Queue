/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.openhft.chronicle.queue;

import org.junit.Rule;
import org.junit.rules.*;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class ChronicleQueueTestBase {
    protected static final Logger LOGGER = LoggerFactory.getLogger(ChronicleQueueTestBase.class);
    protected static final boolean TRACE_TEST_EXECUTION = Boolean.getBoolean("chronicle.traceTestExecution");

    // *************************************************************************
    // JUNIT Rules
    // *************************************************************************

    @Rule
    public final TestName testName = new TestName();

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder(new File(System.getProperty("java.io.tmpdir")));

    @Rule
    public final ErrorCollector errorCollector = new ErrorCollector();

    @Rule
    public TestRule watcher = new TestWatcher() {
        @Override
        protected void starting(Description description) {
            if(TRACE_TEST_EXECUTION) {
                LOGGER.info("Starting test: {}.{}",
                    description.getClassName(),
                    description.getMethodName()
                );
            }
        }
    };

    // *************************************************************************
    //
    // *************************************************************************

    protected File getTmpFile() {
        return getTmpFile(null);
    }

    protected File getTmpFile(String qualifier) {
        try {
            File tmpFile = Files.createTempFile(
                getClass().getSimpleName() + "-",
                "-" + ((qualifier != null && !qualifier.isEmpty())
                    ? testName.getMethodName() + "-" + qualifier
                    : testName.getMethodName()))
                .toFile();

            LOGGER.info("Tmp file: {}", tmpFile);
            return tmpFile;
        } catch(IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
