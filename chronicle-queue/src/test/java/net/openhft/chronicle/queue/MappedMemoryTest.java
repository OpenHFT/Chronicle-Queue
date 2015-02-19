package net.openhft.chronicle.queue;

import net.openhft.lang.io.*;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


public class MappedMemoryTest {


    public static final long SHIFT = 27L;
    long TIMES = 1L << SHIFT;

    @Test
    public void withMappedNativeBytesTest() throws IOException {

        File tempFile = File.createTempFile("chronicle", "q");
        try {

            MappedFile mappedFile = new MappedFile(tempFile.getName(), TIMES, 0);

            ChronicleUnsafe chronicleUnsafe = new ChronicleUnsafe(mappedFile, SHIFT);
            MappedNativeBytes bytes = new MappedNativeBytes(chronicleUnsafe);
            bytes.writeLong(1, 1);
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

    @Test
    public void mappedMemoryTest() throws IOException {

        File tempFile = File.createTempFile("chronicle", "q");

        int shift = 3;
        MappedFile mappedFile = new MappedFile(tempFile.getName(), 1 << shift, 0);

        ChronicleUnsafe chronicleUnsafe = new ChronicleUnsafe(mappedFile, shift);
        MappedNativeBytes bytes = new MappedNativeBytes(chronicleUnsafe);
        bytes.writeUTF("hello this is some very long text");

        bytes.clear();

        bytes.position(100);
        bytes.writeUTF("hello this is some more long text...................");

        bytes.position(100);
        System.out.println("result=" + bytes.readUTF());

    }

}

