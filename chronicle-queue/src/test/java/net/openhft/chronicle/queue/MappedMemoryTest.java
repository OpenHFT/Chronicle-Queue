package net.openhft.chronicle.queue;

import net.openhft.lang.io.*;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by robaustin on 18/02/15.
 */
public class MappedMemoryTest {


    @Test
    public void mappedMemoryTest() throws IOException {

        File tempFile = File.createTempFile("chronicle", "q");

        MappedFile mappedFile = new MappedFile(tempFile.getName(), 8, 0);

        ChronicleUnsafe chronicleUnsafe = new ChronicleUnsafe(mappedFile);
        MappedNativeBytes bytes = new MappedNativeBytes(chronicleUnsafe);
        bytes.writeUTF("hello this is some very long text");

        bytes.clear();

        bytes.position(100);
        bytes.writeUTF("hello this is some more long text...................");

        bytes.position(100);
        System.out.println("result=" + bytes.readUTF());

    }
}

