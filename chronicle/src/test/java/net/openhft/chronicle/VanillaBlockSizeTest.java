/*
 * Copyright 2015 Higher Frequency Trading
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

import net.openhft.lang.Jvm;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.util.Random;

/**
 * @author luke
 *         Date: 4/7/15
 */
@Ignore
public class VanillaBlockSizeTest {

    @Rule
    public final TemporaryFolder tmpdir = new TemporaryFolder(new File(Jvm.TMP));

    @Rule
    public final TestName testName = new TestName();

    @Ignore
    @Test
    public void testMaxSize() throws IOException {
        final File root = tmpdir.newFolder(testName.getMethodName());
        root.deleteOnExit();

        final int SAMPLE = 1000000; // 10000000
        final int ITEM_SIZE = 8; // 256

        byte[] byteArrays = new byte[ITEM_SIZE];
        new Random().nextBytes(byteArrays);

        Chronicle chronicle = ChronicleQueueBuilder
            .vanilla(root.getAbsolutePath() + "/blocksize-test")
            .dataBlockSize(1073741824)
            .indexBlockSize(1073741824)
            .build();

        ExcerptAppender appender = chronicle.createAppender();

        System.out.println("Test...");
        for (int count=0 ; ; count++) {
            appender.startExcerpt(ITEM_SIZE);
            appender.write(byteArrays, 0, ITEM_SIZE);
            appender.finish();

            if (count % SAMPLE == 0) {
                System.out.println(count + " written");
            }
        }
    }
}
