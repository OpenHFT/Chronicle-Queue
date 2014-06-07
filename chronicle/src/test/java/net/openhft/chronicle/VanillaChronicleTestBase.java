/*
 * Copyright 2014 Higher Frequency Trading
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle;

import net.openhft.lang.io.IOTools;
import org.junit.Rule;
import org.junit.rules.TestName;

public class VanillaChronicleTestBase {
    protected static final String TMP_PATH = System.getProperty("java.io.tmpdir");

    @Rule
    public final TestName testName = new TestName();

    protected String getTestPath() {
        final String path = TMP_PATH + "/" + testName.getMethodName();
        IOTools.deleteDir(path);

        return path;
    }

    protected String getTestPath(String suffix) {
        final String path = TMP_PATH + "/" + testName.getMethodName() + suffix;
        IOTools.deleteDir(path);

        return path;
    }
}
