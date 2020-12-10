/*
 * Copyright 2014 Higher Frequency Trading
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

package net.openhft.chronicle.sandbox.attic;

import net.openhft.lang.io.NativeBytes;
import org.junit.Test;

public class VdsoTest {

    @Test
    public void printVdso() throws IOException, InterruptedException {
        long start = 0;
        long end = 0;
        String maps = "/proc/self/maps";
        if (!new File(maps).exists()) return;
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(maps)));
        try {
            for (String line; (line = br.readLine()) != null; ) {
                if (line.endsWith("[vdso]")) {
                    String[] parts = line.split("[- ]");
                    start = Long.parseLong(parts[0], 16);
                    end = Long.parseLong(parts[1], 16);
                }

               // System.out.println(line);
            }
        } catch (IOException ioe) {
            br.close();
            throw ioe;
        }
       // System.out.printf("vdso %x to %x %n", start, end);
        NativeBytes nb = new NativeBytes(start, end);
        long[] longs = new long[(int) ((end - start) / 8)];
        for (int i = 0; i < longs.length; i++)
            longs[i] = nb.readLong(i * 8);
        Jvm.pause(1);
        for (int i = 0; i < longs.length; i++) {
            long l = nb.readLong(i * 8);
            if (l != longs[i])
               // System.out.printf("%d: %d %x%n", i, l, l);
        }
    }
}
