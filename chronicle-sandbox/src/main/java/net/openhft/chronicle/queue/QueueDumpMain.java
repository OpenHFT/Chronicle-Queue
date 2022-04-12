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
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.wire.Wires;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

public class QueueDumpMain {
    static final String[] SBP_TYPES = "data,meta-data,not-ready-data,not-ready-meta-data".split(",");

    public static void dump(File filename, PrintWriter pw) throws FileNotFoundException {
        MappedFile mappedFile = MappedFile.mappedFile(filename, 64 << 20, 16 << 20);
        Bytes<?> bytes = mappedFile.bytes();
        pw.print("# Magic: ");
        for (int i = 0; i < 8; i++)
            pw.print((char) bytes.readUnsignedByte());
        pw.println();
        while (true) {
            long spb = bytes.readUnsignedInt();
            if (!Wires.isKnownLength(spb))
                break;
            pw.print("--- !");
            pw.print(SBP_TYPES[((int) (spb >>> 30))]);
            pw.println();
            long start = bytes.position();
            BytesUtil.toString(bytes, pw, start, start, start + Wires.lengthOf(spb));
            pw.println();
            bytes.skip(Wires.lengthOf(spb));
        }
        pw.flush();
    }
}
