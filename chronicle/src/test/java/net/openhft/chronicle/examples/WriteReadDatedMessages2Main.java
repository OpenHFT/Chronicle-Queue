/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
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

package net.openhft.chronicle.examples;

import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.VanillaChronicle;
import net.openhft.chronicle.VanillaChronicleConfig;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.Date;

/*
Writing 1,000,000,000, the chronicle took 345.815 seconds
Reading 1,000,000,000, the chronicle uses 66409812 KB, took 213.442 seconds
 */
public class WriteReadDatedMessages2Main {
    public static void main(String... ignored) throws IOException, InterruptedException {
        String basePath = "/tmp/index";
        VanillaChronicleConfig config = new VanillaChronicleConfig();
        System.out.println("cycleFormat " + config.cycleFormat());
        System.out.println("cycleLength " + config.cycleLength());
        System.out.println("dataBlockSize " + config.dataBlockSize());
        System.out.println("defaultMessageSize " + config.defaultMessageSize());
        System.out.println("entriesPerCycle " + config.entriesPerCycle());
        System.out.println("indexBlockSize " + config.indexBlockSize());
        System.out.println("synchronous " + config.synchronous());

        VanillaChronicle chronicle = new VanillaChronicle(basePath);
        long messages = 10 * 1000 * 1000L;// 1000 * 50000;
        chronicle.clear();
        long start = System.nanoTime();
        VanillaChronicle.VanillaAppender appender = chronicle.createAppender();
        String msg = "writer1 " + new Date();
        for (long i = 0; i < messages; i++) {
            appender.startExcerpt();
            appender.writeLong(i);
            appender.writeLong(System.currentTimeMillis());
            appender.writeUTFΔ(msg);
            appender.finish();
        }

        System.out.println(Long.toHexString(chronicle.lastWrittenIndex()));
        System.out.printf("Writing %,d, the chronicle took %.3f seconds%n",
                messages, (System.nanoTime() - start) / 1e9);

        ExcerptTailer tailer = chronicle.createTailer();
        StringBuilder msgBuffer = new StringBuilder();
        start = System.nanoTime();
        long counter = 0;
        while (tailer.nextIndex()) {
            long index = tailer.readLong();
            long timeStamp = tailer.readLong();
            tailer.readUTFΔ(msgBuffer);
            tailer.finish();
            counter++;
        }
        long time = System.nanoTime() - start;

        System.out.printf("Reading %,d, the chronicle uses %s KB, took %.3f seconds%n",
                counter, run("du", "-s", basePath).split("\\s")[0], time / 1e9);
    }

    static String run(String... cmd) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        InputStreamReader reader = new InputStreamReader(p.getInputStream());
        StringWriter sw = new StringWriter();
        char[] chars = new char[512];
        for (int len; (len = reader.read(chars)) > 0; ) {
            sw.write(chars, 0, len);
        }
        int exitValue = p.waitFor();
        if (exitValue != 0) {
            sw.write("\nexit=" + exitValue);
        }
        p.destroy();
        return sw.toString();
    }
}