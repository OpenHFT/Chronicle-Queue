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

import org.junit.Test;

import static org.junit.Assert.fail;

public class VanillaChronicleConfigTest extends VanillaChronicleTestBase {

    @Test
    public void testVanillaChronicleConfig()  {
        try {
            System.out.print("Check entriesPerCycle >= 256: ");
            new VanillaChronicleConfig().entriesPerCycle(128);
            fail("expected IllegalArgumentException (entriesPerCycle >= 256");
        } catch(IllegalArgumentException e) {
            System.out.print(" OK\n");
        }

        try {
            System.out.print("Check entriesPerCycle <= 1L << 48: ");
            new VanillaChronicleConfig().entriesPerCycle(1L << 56);
            fail("expected IllegalArgumentException (entriesPerCycle <= 1L << 48)");
        } catch(IllegalArgumentException e) {
            System.out.print(" OK\n");
        }

        try {
            System.out.print("Check entriesPerCycle is a power of 2: ");
            new VanillaChronicleConfig().entriesPerCycle(257);
            fail("expected IllegalArgumentException (entriesPerCycle power of 2)");
        } catch(IllegalArgumentException e) {
            System.out.print(" OK\n");
        }

        new VanillaChronicleConfig().entriesPerCycle(512);
        new VanillaChronicleConfig().entriesPerCycle(1024);
    }
}
