package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.wire.Wires;

import java.io.File;
import java.io.IOException;

/**
 * Created by Peter on 07/03/2016.
 */
public class DumpQueueMain {
    public static void main(String[] args) {
        dump(args[0]);
    }

    public static void dump(String dir) {
        File[] files = new File(dir).listFiles();
        if (files == null) {
            System.err.println("Directory not found " + dir);
        }
        for (File file : files) {
            if (file.getName().endsWith(SingleChronicleQueue.SUFFIX)) {
                try (MappedBytes bytes = MappedBytes.mappedBytes(file, 4 << 20)) {
                    System.out.println(Wires.fromSizePrefixedBlobs(bytes));
                } catch (IOException ioe) {
                    System.err.println("Failed to read " + file + " " + ioe);
                }
            }
        }
    }
}
