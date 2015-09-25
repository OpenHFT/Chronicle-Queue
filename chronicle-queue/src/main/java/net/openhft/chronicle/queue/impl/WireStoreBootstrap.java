/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.openhft.chronicle.queue.impl;

public class WireStoreBootstrap {

    public void boot() {
        /*
        String masterFile = OS.TARGET + "/wired-file-" + System.nanoTime();
        for (int i = 1; i <= 5; i++) {
            WiredFile<MyHeader_1_0> wf = WiredFile.build(masterFile,
                file -> MappedFile.mappedFile(file, 64 << 10, 0),
                WireType.BINARY,
                MyHeader_1_0::new,
                wf0 -> wf0.delegate().install(wf0)
            );
            MyHeader_1_0 header = wf.delegate();
            assertEquals(i, header.installCount.getValue());
            Bytes<?> bytes = wf.acquireWiredChunk(0).bytes();
            bytes.readPosition(0);
            bytes.readLimit(wf.headerLength());
            System.out.println(Wires.fromSizePrefixedBlobs(bytes));
            wf.close();
        }
        */
    }
}
