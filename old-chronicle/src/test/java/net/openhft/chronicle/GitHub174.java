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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class GitHub174 {

    @Parameterized.Parameters
    public static Collection dataBlockSize() {
        return Arrays.asList(new Object[][] {
                {  64               },
                {  64 * 128         },
                { 128 * 1024        },
                {  32 * 1024 * 1024 },
                {  64 * 1024 * 1024 },
                { 128 * 1024 * 1024 },
                { 256 * 1024 * 1024 }
        });
    }

    private final int dataBlockSize;

    public GitHub174(int dataBlockSize) {
        this.dataBlockSize = dataBlockSize;
    }

    @Test
    public void testError() throws Exception {
        String tmpdir = System.getProperty("java.io.tmpdir");
        String path = tmpdir + "/error_" + this.dataBlockSize;

        ChronicleTools.deleteOnExit(path);

        write(path, 2);
        write(path, 2);
        assertEquals(4, readCount(path));
    }

    private void write(String path, int count) throws IOException {
        try (Chronicle chronicle = chronicle(path)) {
            ExcerptAppender appender = chronicle.createAppender();
            for (int i = 0; i < count; i++) {
                appender.startExcerpt(32);
                appender.writeInt(0, i);
                appender.position(32);
                appender.finish();
            }
            appender.close();
        }
    }

    private int readCount(String path) throws IOException {
        try (Chronicle chronicle = chronicle(path)) {
            ExcerptTailer tailer = chronicle.createTailer();
            int i = 0;
            while (tailer.nextIndex()) {
                tailer.readInt();
                tailer.finish();
                i++;
            }
            tailer.close();
            return i;
        }
    }

    private Chronicle chronicle(String path) throws IOException {
        return ChronicleQueueBuilder
            .indexed(path)
            .dataBlockSize(this.dataBlockSize)
            .build();
    }
}
