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

import static org.junit.Assert.fail;

public class VanillaChronicleQueueBuilderTest extends VanillaChronicleTestBase {

    @Test
    public void testVanillaChronicleQueueBuilder()  {
        ChronicleQueueBuilder.VanillaChronicleQueueBuilder builder = ChronicleQueueBuilder.vanilla(
            System.getProperty("java.io.tmpdir")
        );

        try {
            System.out.print("Check entriesPerCycle >= 256: ");
            builder.entriesPerCycle(128);
            fail("expected IllegalArgumentException (entriesPerCycle >= 256");
        } catch(IllegalArgumentException e) {
            System.out.print(" OK <" + e.getMessage() + ">\n");
        }

        try {
            System.out.print("Check entriesPerCycle <= 1L << 48: ");
            builder.entriesPerCycle(1L << 56);
            fail("expected IllegalArgumentException (entriesPerCycle <= 1L << 48)");
        } catch(IllegalArgumentException e) {
            System.out.print(" OK <" + e.getMessage() + ">\n");
        }

        try {
            System.out.print("Check entriesPerCycle is a power of 2: ");
            builder.entriesPerCycle(257);
            fail("expected IllegalArgumentException (entriesPerCycle power of 2)");
        } catch(IllegalArgumentException e) {
            System.out.print(" OK <" + e.getMessage() + ">\n");
        }

        builder.entriesPerCycle(512);
        builder.entriesPerCycle(1024);
    }
}
