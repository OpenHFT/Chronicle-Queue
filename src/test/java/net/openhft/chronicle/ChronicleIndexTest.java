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

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class ChronicleIndexTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws IOException {
        VanillaChronicle chronicle = (VanillaChronicle)ChronicleQueueBuilder.vanilla(folder.newFolder()).build();
        VanillaChronicle.VanillaAppender appender = chronicle.createAppender();
        appender.startExcerpt();
        appender.writeUTF("This is a test");
        appender.finish();

        long lastIndex = appender.lastWrittenIndex();
        Assert.assertTrue("lastIndex: "+ lastIndex, lastIndex >= 0);
        Assert.assertEquals(lastIndex+1, appender.index());

        Excerpt excerpt = chronicle.createExcerpt();
        Assert.assertTrue(excerpt.index(lastIndex));
        String utf = excerpt.readUTF();

        assertThat(utf, equalTo("This is a test"));
        folder.delete();
    }
}