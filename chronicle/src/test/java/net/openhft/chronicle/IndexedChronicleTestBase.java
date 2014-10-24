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

package net.openhft.chronicle;

import net.openhft.chronicle.tools.ChronicleTools;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.File;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class IndexedChronicleTestBase {
    protected static final String TMP_DIR = System.getProperty("java.io.tmpdir");

    @Rule
    public final TestName testName = new TestName();

    protected synchronized String getTestPath() {
        final String path = TMP_DIR + "/ic-" + testName.getMethodName();
        ChronicleTools.deleteOnExit(path);

        return path;
    }

    protected synchronized String getTestPath(String suffix) {
        final String path = TMP_DIR + "/ic-" + testName.getMethodName() + suffix;
        ChronicleTools.deleteOnExit(path);

        return path;
    }

    protected static void assertExists(String path) {
        assertNotNull(path);
        assertTrue(new File(path + ".index").exists());
        assertTrue(new File(path + ".data").exists());
    }

    protected static void assertNotExists(String path) {
        assertNotNull(path);
        assertFalse(new File(path + ".index").exists());
        assertFalse(new File(path + ".data").exists());
    }

    protected static void assertClean(String path) {
        assertNotNull(path);
        assertTrue(new File(path + ".index").delete());
        assertTrue(new File(path + ".data").delete());
    }
}
