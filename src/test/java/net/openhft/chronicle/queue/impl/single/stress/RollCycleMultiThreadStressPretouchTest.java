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

package net.openhft.chronicle.queue.impl.single.stress;

import org.junit.Test;

public class RollCycleMultiThreadStressPretouchTest extends RollCycleMultiThreadStressTest {

    public RollCycleMultiThreadStressPretouchTest() {
        super(StressTestType.PRETOUCH);
    }

    @Test
    public void stress() throws Exception {
        expectException("Creating cycle files from the Pretoucher is not supported in this release");
        expectException("This functionality has been deprecated and in future will only be available in Chronicle Queue Enterprise");
        super.stress();
    }

    public static void main(String[] args) throws Exception {
        new RollCycleMultiThreadStressPretouchTest().stress();
    }
}