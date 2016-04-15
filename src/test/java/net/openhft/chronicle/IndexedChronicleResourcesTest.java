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

import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class IndexedChronicleResourcesTest extends IndexedChronicleTestBase {
    @Test
    public void resourceTest1() throws IOException {
        final String basePath = "simpleTest";
        deleteAndAssert(basePath);

        Chronicle chronicle = ChronicleQueueBuilder.indexed(basePath).build();
        chronicle.close();
        deleteAndAssert(basePath);
    }

    @Test
    public void resourceTest2() throws IOException {
        final String basePath = "simpleTest";
        deleteAndAssert(basePath);

        Chronicle chronicle = ChronicleQueueBuilder.indexed(basePath).build();
        ExcerptAppender appender = chronicle.createAppender();
        appender.close();
        chronicle.close();

        // it must be called because of JDK-6558368
        System.gc();
        deleteAndAssert(basePath);
    }

    private void deleteAndAssert(String basePath) throws IOException {
        for (String name : new String[] { basePath + ".data", basePath + ".index" }) {
            File file = new File(name);
            if (file.exists()) {
                assertTrue(file.getCanonicalPath(), file.delete());
            }
        }
    }
}
