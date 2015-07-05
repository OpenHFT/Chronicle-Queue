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

import static org.junit.Assert.assertEquals;

public class GitHub174 {
    @Test
    public void testError() throws Exception {
        String dir = System.getProperty("java.io.tmpdir");
        if (new File(dir, "error.data").exists()) {
            new File(dir, "error.data").delete();
            new File(dir, "error.index").delete();
        }

        write(dir, 2);
        write(dir, 2);
        assertEquals(4, readCount(dir));
    }

    private void write(String dir, int count) throws IOException {
        try (Chronicle chronicle = chronicle(dir)) {
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

    private int readCount(String dir) throws IOException {
        try (Chronicle chronicle = chronicle(dir)) {
            ExcerptTailer tailer = chronicle.createTailer();
            int i = 0;
            while (tailer.nextIndex()) {
                System.out.println(tailer.readInt());
                i++;
            }
            tailer.close();
            return i;
        }
    }

    private Chronicle chronicle(String dir) throws IOException {
        return ChronicleQueueBuilder
                .indexed(dir, "error")
                .dataBlockSize(64)
                .indexBlockSize(64)
                .build();
    }
}
