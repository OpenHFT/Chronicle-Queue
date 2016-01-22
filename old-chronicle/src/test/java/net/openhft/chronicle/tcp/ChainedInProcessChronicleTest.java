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

import net.openhft.chronicle.IndexedChronicleTestBase;

/**
 * @author peter.lawrey
 */
public class ChainedInProcessChronicleTest extends IndexedChronicleTestBase {

    /*
    @Ignore
    @Test
    public void testChained() throws IOException {
        final String basePath1 = getTestPath("-1");
        final String basePath2 = getTestPath("-2");
        final String basePath3 = getTestPath("-3");

        Chronicle source1 = new ChronicleSource(new IndexedChronicle(basePath1), 61111);
        Chronicle source2 = new ChronicleSource( new IndexedChronicle(basePath2), 62222);
        Chronicle sink2 = new ChronicleSink(source2, "localhost", 61111);
        Chronicle sink3 = new ChronicleSink(new IndexedChronicle(basePath3), "localhost", 62222);

        ExcerptAppender excerpt1 = source1.createAppender();
        ExcerptTailer excerpt2 = sink2.createTailer();
        ExcerptTailer excerpt3 = sink3.createTailer();

        for (int i = 1; i < 20; i++) {
            excerpt1.startExcerpt();
            excerpt1.writeLong(System.nanoTime());
            excerpt1.finish();

            while (excerpt2.size() < i) {
                excerpt2.nextIndex();
            }

            while (excerpt3.size() < i) {
                excerpt3.nextIndex();
            }
        }

        sink3.close();
        sink2.close();
        source1.close();

        assertClean(basePath1);
        assertClean(basePath2);
        assertClean(basePath3);
    }

    @Ignore //TODO fix this case
    @Test
    public void testChainedChronicleReconnection() throws IOException, InterruptedException {
        final String basePath1 = getTestPath("-1");
        final String basePath2 = getTestPath("-2");

        Chronicle source = new ChronicleSource(new IndexedChronicle(basePath1), 61111);

        //write some data into the 'source' chronicle
        ExcerptAppender sourceAppender = source.createAppender();
        long NUM_INITIAL_MESSAGES = 20;
        for (long i = 0; i < NUM_INITIAL_MESSAGES; i++) {
            sourceAppender.startExcerpt();
            sourceAppender.writeLong(i);
            sourceAppender.flush();
            sourceAppender.finish();
        }

        // Starting first slave instance
        // create the 'slave' chronicle

        Chronicle source1 = new ChronicleSource(new IndexedChronicle(basePath2), 62222);
        Chronicle sink1 = new ChronicleSink(source1, "localhost", 61111);

        //try to read current data from the 'slave' chronicle

        ExcerptTailer tailer1 = sink1.createTailer();
        long nextIndex1 = 0;
        while (tailer1.nextIndex()) {
            assertEquals("Unexpected index in stream", tailer1.readLong(), nextIndex1++);
        }
        assertEquals("Unexpected number of messages in stream", NUM_INITIAL_MESSAGES, nextIndex1);

        // Close first 'slave' chronicle

        sink1.close();
        source1.close();

        // Write some more data

        for (long i = NUM_INITIAL_MESSAGES; i < NUM_INITIAL_MESSAGES * 2; i++) {
            sourceAppender.startExcerpt();
            sourceAppender.writeLong(i);
            sourceAppender.flush();
            sourceAppender.finish();
        }

        // Starting second slave instance
        // Observe that we don't call ChronicleTools.deleteOnExit(file) -
        // the new instance will re-open the existing chronicle file
        Chronicle source2 = new ChronicleSource(new IndexedChronicle(basePath2), 63333);
        Chronicle sink2 = new ChronicleSink(source2, "localhost", 61111);

        ExcerptTailer tailer2 = sink2.createTailer();
        long nextIndex2 = 0;
        while (tailer2.nextIndex()) {
            assertEquals("Unexpected message index in stream", tailer2.readLong(), nextIndex2++);
        }

        assertEquals("Didn't read all messages", NUM_INITIAL_MESSAGES * 2, nextIndex2);

        // Cleaning up
        sink2.close();
        source2.close();

        source.close();

        assertClean(basePath1);
        assertClean(basePath2);
    }
    */
}
