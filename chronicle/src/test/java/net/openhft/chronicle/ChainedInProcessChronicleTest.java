/*
 * Copyright 2013 Peter Lawrey
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

import net.openhft.chronicle.tcp.InProcessChronicleSink;
import net.openhft.chronicle.tcp.InProcessChronicleSource;
import net.openhft.chronicle.tools.ChronicleTools;
import org.junit.Test;

import static org.junit.Assert.*;


import java.io.IOException;

/**
 * @author peter.lawrey
 */
public class ChainedInProcessChronicleTest {
    private static final String TMP = System.getProperty("java.io.tmpdir");

    @Test
    public void testChained() throws IOException {
        ChronicleTools.deleteOnExit(TMP + "/chronicle1");
        Chronicle chronicle1 = new IndexedChronicle(TMP + "/chronicle1");
        InProcessChronicleSource source1 = new InProcessChronicleSource(chronicle1, 61111);

        ChronicleTools.deleteOnExit(TMP + "/chronicle2");
        Chronicle chronicle2 = new IndexedChronicle(TMP + "/chronicle2");
        InProcessChronicleSource source2 = new InProcessChronicleSource(chronicle2, 62222);
        InProcessChronicleSink sink2 = new InProcessChronicleSink(source2, "localhost", 61111);

        ChronicleTools.deleteOnExit(TMP + "/chronicle3");
        Chronicle chronicle3 = new IndexedChronicle(TMP + "/chronicle3");
        InProcessChronicleSink sink3 = new InProcessChronicleSink(chronicle3, "localhost", 62222);

        ExcerptAppender excerpt1 = source1.createAppender();
        ExcerptTailer excerpt2 = sink2.createTailer();
        ExcerptTailer excerpt3 = sink3.createTailer();

        for (int i = 1; i < 20; i++) {
            excerpt1.startExcerpt();
            excerpt1.writeLong(System.nanoTime());
            excerpt1.finish();

            while (excerpt2.size() < i)
                excerpt2.nextIndex();

            while (excerpt3.size() < i)
                excerpt3.nextIndex();
        }

        sink3.close();
        sink2.close();
        source1.close();
    }

    @Test
    public void testChainedChronicleReconnection() throws IOException, InterruptedException {

        //create the 'source' chronicle
        ChronicleTools.deleteOnExit(TMP + "/chronicle1");
        Chronicle chronicle = new IndexedChronicle(TMP + "/chronicle1");
        InProcessChronicleSource chronicleSource = new InProcessChronicleSource(chronicle, 61111);

        //write some data into the 'source' chronicle
        ExcerptAppender sourceAppender = chronicleSource.createAppender();
        long NUM_INITIAL_MESSAGES = 20;
        for (long i = 0; i < NUM_INITIAL_MESSAGES; i++) {
            sourceAppender.startExcerpt();
            sourceAppender.writeLong(i);
            sourceAppender.flush();
            sourceAppender.finish();
        }

        // Starting first slave instance
        // create the 'slave' chronicle

        ChronicleTools.deleteOnExit(TMP + "/chronicle2");
        Chronicle chronicle1 = new IndexedChronicle(TMP + "/chronicle2");
        InProcessChronicleSource chronicleSource1 = new InProcessChronicleSource(chronicle1, 62222);
        InProcessChronicleSink chronicleSink1 = new InProcessChronicleSink(chronicleSource1, "localhost", 61111);

        //try to read current data from the 'slave' chronicle

        ExcerptTailer tailer1 = chronicleSink1.createTailer();
        long nextIndex1 = 0;
        while (tailer1.nextIndex()) {
            assertEquals("Unexpected index in stream", tailer1.readLong(), nextIndex1++);
        }
        assertEquals("Unexpected number of messages in stream", NUM_INITIAL_MESSAGES, nextIndex1);

        // Close first 'slave' chronicle

        chronicleSink1.close();
        chronicleSource1.close();
        chronicle1.close();

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
        Chronicle chronicle2 = new IndexedChronicle(TMP + "/chronicle2");
        InProcessChronicleSource chronicleSource2 = new InProcessChronicleSource(chronicle2, 63333);
        InProcessChronicleSink chronicleSink2 = new InProcessChronicleSink(chronicleSource2, "localhost", 61111);

        ExcerptTailer tailer2 = chronicleSink2.createTailer();
        long nextIndex2 = 0;
        while (tailer2.nextIndex()) {
            assertEquals("Unexpected message index in stream", tailer2.readLong(), nextIndex2++);
        }

        assertEquals("Didn't read all messages", NUM_INITIAL_MESSAGES * 2, nextIndex2);

        // Cleaning up
        chronicleSink2.close();
        chronicleSource2.close();
        chronicle2.close();

        chronicleSource.close();
        chronicle.close();
    }
}
