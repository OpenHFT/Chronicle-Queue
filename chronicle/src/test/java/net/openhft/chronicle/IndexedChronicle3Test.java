/*
 * Copyright 2014 Higher Frequency Trading
 * <p/>
 * http://www.higherfrequencytrading.com
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle;

import net.openhft.chronicle.tools.CheckedExcerpt;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IndexedChronicle3Test extends IndexedChronicleTestBase {

    @Test
    public void testCheckedIndexedExcerpt_001() throws IOException {
        final ChronicleConfig cfg = ChronicleConfig.DEFAULT.clone();
        cfg.useCheckedExcerpt(true);

        final String basePath = getTestPath();
        final Chronicle chronicle = new IndexedChronicle(basePath,cfg);
        final ExcerptAppender appender = chronicle.createAppender();

        assertTrue(appender instanceof CheckedExcerpt);

        try {
            appender.startExcerpt(8);
            appender.writeLong(1);

            testByte(appender);
            testChar(appender);
            testShort(appender);
            testInt(appender);
            testLong(appender);
            testDouble(appender);
            testObject(appender);

            appender.finish();
        } finally {
            appender.close();
            chronicle.close();

            assertClean(basePath);
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    private void testByte(final ExcerptAppender appender) {
        try {
            appender.writeByte(1);
            fail("expected IllegalStateException");
        } catch(IllegalStateException e) {
            // expected
        }

        try {
            appender.writeByte(8,1);
            fail("expected IllegalStateException");
        } catch(IllegalStateException e) {
            // expected
        }
    }

    private void testChar(final ExcerptAppender appender) {
        try {
            appender.writeChar(1);
            fail("expected IllegalStateException");
        } catch(IllegalStateException e) {
            // expected
        }

        try {
            appender.writeChar(8,1);
            fail("expected IllegalStateException");
        } catch(IllegalStateException e) {
            // expected
        }
    }

    private void testShort(final ExcerptAppender appender) {
        try {
            appender.writeShort(1);
            fail("expected IllegalStateException");
        } catch(IllegalStateException e) {
            // expected
        }

        try {
            appender.writeShort(8,1);
            fail("expected IllegalStateException");
        } catch(IllegalStateException e) {
            // expected
        }
    }

    private void testInt(final ExcerptAppender appender) {
        try {
            appender.writeInt(1);
            fail("expected IllegalStateException");
        } catch(IllegalStateException e) {
            // expected
        }

        try {
            appender.writeOrderedInt(1);
            fail("expected IllegalStateException");
        } catch(IllegalStateException e) {
            // expected
        }

        try {
            appender.writeInt(8,1);
            fail("expected IllegalStateException");
        } catch(IllegalStateException e) {
            // expected
        }

        try {
            appender.writeOrderedInt(8,1);
            fail("expected IllegalStateException");
        } catch(IllegalStateException e) {
            // expected
        }

        try {
            appender.compareAndSwapInt(8,1,2);
            fail("expected IllegalStateException");
        } catch(IllegalStateException e) {
            // expected
        }
    }

    private void testLong(final ExcerptAppender appender) {
        try {
            appender.writeLong(1);
            fail("expected IllegalStateException");
        } catch(IllegalStateException e) {
            // expected
        }

        try {
            appender.writeOrderedLong(1);
            fail("expected IllegalStateException");
        } catch(IllegalStateException e) {
            // expected
        }

        try {
            appender.writeLong(8,1);
            fail("expected IllegalStateException");
        } catch(IllegalStateException e) {
            // expected
        }

        try {
            appender.writeOrderedLong(8,1);
            fail("expected IllegalStateException");
        } catch(IllegalStateException e) {
            // expected
        }

        try {
            appender.compareAndSwapLong(8,1,2);
            fail("expected IllegalStateException");
        } catch(IllegalStateException e) {
            // expected
        }
    }

    private void testDouble(final ExcerptAppender appender) {
        try {
            appender.writeDouble(1);
            fail("expected IllegalStateException");
        } catch(IllegalStateException e) {
            // expected
        }

        try {
            appender.writeDouble(8,1);
            fail("expected IllegalStateException");
        } catch(IllegalStateException e) {
            // expected
        }
    }

    private void testObject(final ExcerptAppender appender) {
        try {
            appender.writeObject("1234567890",0,5);
            fail("expected IllegalStateException");
        } catch(IllegalStateException e) {
            // expected
        }
    }
}
