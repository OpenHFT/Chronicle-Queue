package net.openhft.chronicle.queue;

import net.openhft.lang.io.*;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by robaustin on 18/02/15.
 */
public class MappedMemoryTest {


    long TIMES = 1L << 25L;

    @Test
    public void withMappedNativeBytesTest() throws IOException {

        File tempFile = File.createTempFile("chronicle", "q");
        try {

            MappedFile mappedFile = new MappedFile(tempFile.getName(), TIMES, 0);

            ChronicleUnsafe chronicleUnsafe = new ChronicleUnsafe(mappedFile);
            MappedNativeBytes bytes = new MappedNativeBytes(chronicleUnsafe);

            long startTime = System.nanoTime();
            for (long i = 0; i < TIMES; i += 8) {
                bytes.writeLong(i);
            }

            System.out.println("With MappedNativeBytes,\t time=" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime) + ("ms, number of longs written=" + TIMES / 8));

        } finally {
            tempFile.delete();
        }

    }

    @Test
    public void withRawNativeBytesTess() throws IOException {

        File tempFile = File.createTempFile("chronicle", "q");
        try {

            MappedFile mappedFile = new MappedFile(tempFile.getName(), TIMES, 0);
            Bytes bytes1 = mappedFile.acquire(1).bytes();

            long startTime = System.nanoTime();
            for (long i = 0; i < TIMES; i += 8L) {
                bytes1.writeLong(i);
            }

            System.out.println("With NativeBytes,\t\t time=" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime) + ("ms, number of longs written=" + TIMES / 8));


        } finally {
            tempFile.delete();
        }

    }
}

