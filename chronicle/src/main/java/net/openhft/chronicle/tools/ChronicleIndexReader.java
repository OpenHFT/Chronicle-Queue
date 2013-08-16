package net.openhft.chronicle.tools;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

/**
 * User: peter
 * Date: 16/08/13
 * Time: 12:30
 */
public class ChronicleIndexReader {
    static final boolean HEX = Boolean.getBoolean("hex");

    public static void main(String... args) throws IOException {
        FileChannel fc = new FileInputStream(args[0]).getChannel();
        ByteBuffer buffer = ByteBuffer.allocateDirect(4096).order(ByteOrder.nativeOrder());
        while (fc.read(buffer) > 0) {
            for (int i = 0; i < buffer.capacity(); i += 4 * 16) {
                long indexStart = buffer.getLong(i);
                if (indexStart == 0) continue;
                System.out.print(HEX ? Long.toHexString(indexStart) : "" + indexStart);
                for (int j = i + 8; j < i + 64; j += 4) {
                    System.out.print(' ');
                    int offset = buffer.getInt(j);
                    System.out.print(HEX ? Integer.toHexString(offset) : "" + offset);
                }
                System.out.println();
            }
            buffer.clear();
        }
        fc.close();
    }
}
