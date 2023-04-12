/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.MethodFilterOnFirstArg;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class IgnoreMethodBasedOnFirstArgTest extends QueueTestCommon {

    private static final String EXPECTED_ENVELOPE = "for:rob";
    private static final String MSG = "hello world";

    interface Printer extends MethodFilterOnFirstArg {
        void print(String envelope, String msg);
    }

    @Test
    public void testIgnoreMethodBasedOnFirstArg() {
        try (SingleChronicleQueue build = SingleChronicleQueueBuilder.binary(DirectoryUtils.tempDir("q")).build()) {
            Printer printer = build.acquireAppender().methodWriter(Printer.class);
            printer.print(EXPECTED_ENVELOPE, MSG);
            MethodReader mr = build.createTailer().methodReaderBuilder().build(
                    new Printer() {

                        @Override
                        public boolean ignoreMethodBasedOnFirstArg(final String methodName, final Object firstArg) {
                            assertEquals(EXPECTED_ENVELOPE, firstArg);
                            return false;
                        }

                        @Override
                        public void print(String envelope, final String msg) {
                            assertEquals(EXPECTED_ENVELOPE, envelope);
                            assertEquals(MSG, msg);
                        }
                    });
            mr.readOne();
        }
    }
}

