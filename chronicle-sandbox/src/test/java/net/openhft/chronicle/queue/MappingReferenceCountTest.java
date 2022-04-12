/*
 * Copyright 2015 Higher Frequency Trading
 *
 * http://chronicle.software
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

package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedFile;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;

import static org.junit.Assert.assertEquals;

public class MappingReferenceCountTest {

    /**
     * tests that blocks are acquired and released as needed
     *
     * @
     */
    @Test
    public void testMappingReferenceCount() {

        File tempFile = File.createTempFile("chronicle", "q");

        try {
            int BLOCK_SIZE = 4096;
            final MappedFile mappedFile = MappedFile.mappedFile(tempFile.getName(), BLOCK_SIZE, 8);
            final Bytes<?> bytes = mappedFile.bytes();

            // write into block 1
            bytes.writeLong(4096 + 8, Long.MAX_VALUE);
           // Assert.assertEquals(1, mappedFile.getRefCount(1));
            assertEquals("refCount: 2, 0, 2", mappedFile.referenceCounts());

            // we move from block 1 to block 2
            bytes.writeLong((4096 * 2) + 8, Long.MAX_VALUE);
           // assertEquals(0, mappedFile.getRefCount(1));
           // assertEquals(1, mappedFile.getRefCount(2));
            assertEquals("refCount: 3, 0, 1, 2", mappedFile.referenceCounts());

            // we move from block 2 back to block 1
            bytes.writeLong((4096 * 1) + 8, Long.MAX_VALUE);
           // assertEquals(1, mappedFile.getRefCount(1));
           // assertEquals(0, mappedFile.getRefCount(2));
            assertEquals("refCount: 3, 0, 2, 1", mappedFile.referenceCounts());

            // we move from block 2 back to block 1
            bytes.writeLong((4096 * 3) + 8, Long.MAX_VALUE);
           // assertEquals(1, mappedFile.getRefCount(3));
            assertEquals("refCount: 4, 0, 1, 1, 2", mappedFile.referenceCounts());

            bytes.releaseLast();
            mappedFile.close();

            assertEquals("refCount: 0, 0, 0, 0, 0", mappedFile.referenceCounts());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            tempFile.delete();
        }
    }
}
