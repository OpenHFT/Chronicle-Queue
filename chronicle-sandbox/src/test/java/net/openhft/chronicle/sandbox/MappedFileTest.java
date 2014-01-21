/*
 * Copyright 2013 Peter Lawrey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.sandbox;

import net.openhft.lang.io.MappedFile;
import net.openhft.lang.io.MappedMemory;
import org.junit.Test;

import java.io.*;
import java.nio.MappedByteBuffer;

import static org.junit.Assert.assertTrue;

public class MappedFileTest {
    public static void printMappings() throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("/proc/self/maps")));
        try {
            for (String line; (line = br.readLine()) != null; ) {
                System.out.println(line);
            }
        } catch (IOException ioe) {
            br.close();
            throw ioe;
        }

    }

    public static void delete(File file) throws IOException {
        if (file.delete() || !file.exists()) return;
        ProcessBuilder pb = new ProcessBuilder("/bin/rm", file.getAbsolutePath());
        pb.redirectErrorStream(true);
        Process p = pb.start();
        StringWriter sw = new StringWriter();
        char[] chars = new char[512];
        Reader r = new InputStreamReader(p.getInputStream());
        for (int len; (len = r.read(chars)) > 0; )
            sw.write(chars, 0, len);
        String msg = sw.toString().trim();
        if (msg.length() > 0)
            throw new IOException(msg);
    }

    @Test
    public void testUnmap() throws IOException, InterruptedException {

        String basePath = "/tmp/testUnmap";
        File file = new File(basePath);
        File dir = file.getParentFile();
        long free0 = dir.getFreeSpace();

        MappedFile mfile = new MappedFile(basePath, 1024 * 1024);
        MappedMemory map0 = mfile.acquire(0);
        fill(map0.buffer());
        MappedMemory map1 = mfile.acquire(1);
        fill(map1.buffer().force());
        long free1 = dir.getFreeSpace();

        map1.release();
        map0.release();
        mfile.close();

//        printMappings();
        long free2 = dir.getFreeSpace();
        delete(file);
        long free3 = 0;
        for (int i = 0; i < 100; i++) {
            free3 = dir.getFreeSpace();
            System.out.println("Freed " + free0 + " ~ " + free1 + " ~ " + free2 + " ~ " + free3 + ", delete = " + file.delete());
            if (free3 > free1)
                break;
            Thread.sleep(500);
        }
        assertTrue("free3-free1: " + (free3 - free1), free3 > free1);


    }

    private void fill(MappedByteBuffer buffer) {
        buffer.position(0);
        while (buffer.remaining() >= 8)
            buffer.putLong(0x123456789ABCDEFL);
    }
}
