/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.tcp;


import net.openhft.chronicle.tools.ChronicleTools;
import net.openhft.lang.io.IOTools;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class StatefulChronicleTestBase {
    protected static final Logger LOGGER    = LoggerFactory.getLogger("StatefulChronicleTest");
    protected static final String TMP_DIR   = System.getProperty("java.io.tmpdir");
    protected static final String PREFIX    = "ch-statefull-";
    protected static final int    BASE_PORT = 12000;

    @Rule
    public final TestName testName = new TestName();

    protected synchronized String getIndexedTestPath() {
        final String path = TMP_DIR + "/" + PREFIX + testName.getMethodName();
        ChronicleTools.deleteOnExit(path);

        return path;
    }

    protected synchronized String getIndexedTestPath(String suffix) {
        final String path = TMP_DIR + "/" + PREFIX + testName.getMethodName() + suffix;
        ChronicleTools.deleteOnExit(path);

        return path;
    }

    protected static void assertIndexedClean(String path) {
        assertNotNull(path);
        assertTrue(new File(path + ".index").delete());
        assertTrue(new File(path + ".data").delete());
    }

    protected synchronized String getVanillaTestPath() {
        final String path = TMP_DIR + "/" + PREFIX + testName.getMethodName();
        IOTools.deleteDir(path);

        return path;
    }

    protected synchronized String getVanillaTestPath(String suffix) {
        final String path = TMP_DIR + "/" + PREFIX + testName.getMethodName() + suffix;
        IOTools.deleteDir(path);

        return path;
    }
}
