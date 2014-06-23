/*
 * Copyright 2014 Higher Frequency Trading
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


import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.IndexedChronicle;
import net.openhft.chronicle.VanillaChronicle;
import net.openhft.chronicle.tools.ChronicleTools;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;

public class InMemoryChronicleTestBase {
    protected static final String TMP_DIR   = System.getProperty("java.io.tmpdir");
    protected static final String PREFIX    = "in-memory-";
    protected static final int    BASE_PORT = 12000;

    @Rule
    public final TestName testName = new TestName();

    protected synchronized String getTestPath() {
        final String path = TMP_DIR + "/" + PREFIX + testName.getMethodName();
        ChronicleTools.deleteOnExit(path);

        return path;
    }

    protected synchronized String getTestPath(String suffix) {
        final File path = new File(TMP_DIR + "/" + PREFIX + testName.getMethodName() + suffix);
        if(path.exists()) {
            path.delete();
        }

        return path.getAbsolutePath();
    }

    protected Chronicle indexedChronicleSource(String basePath, int port) throws IOException {
        return new InProcessChronicleSource(new IndexedChronicle(basePath), port);
    }

    protected Chronicle vanillaChronicleSource(String basePath, int port) throws IOException {
        return new VanillaChronicleSource(new VanillaChronicle(basePath), port);
    }
}
