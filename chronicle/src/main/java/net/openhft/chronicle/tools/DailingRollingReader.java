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

package net.openhft.chronicle.tools;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

/**
 * @author peter.lawrey
 */
public class DailingRollingReader {
    public static void main(String[] args) throws IOException {
        dumpData(args[0], new PrintWriter(System.out));
    }

    public static void dumpData(String filename, Writer writer) throws IOException {
        PrintWriter pw = writer instanceof PrintWriter ? ((PrintWriter) writer) : new PrintWriter(writer);
        FileChannel fc = new FileInputStream(filename).getChannel();
        try {
            ByteBuffer bb = ByteBuffer.allocateDirect(4096).order(ByteOrder.nativeOrder());
            long index = 0;
            int size = 0;
            while (true) {
                bb.clear();
                fc.read(bb);
                bb.flip();
                while (bb.remaining() > 0) {
                    // write the remaining data.
                    while (bb.remaining() > 0 && size > 0) {
                        byte b = bb.get();
                        if (b < ' ' || b > 126)
                            b = '.';
                        pw.print((char) b);
                        size--;
                    }
                    if (size == 0) {
                        // skip the padding.
                        while ((bb.position() & 3) != 0)
                            bb.get();
                        if (bb.remaining() >= 4) {
                            size = bb.getInt();
                            pw.println();
                            if (size == 0) {
                                pw.println("Exceprts " + index);
                                return;
                            }
                            pw.print(index++);
                            pw.print(":");
                            pw.print(size);
                            pw.print(":");
                        }
                    }
                }
            }
        } finally {
            fc.close();
        }
    }
}
