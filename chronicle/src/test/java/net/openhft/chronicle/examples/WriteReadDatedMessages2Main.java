/*
 * Copyright 2014 Higher Frequency Trading
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

package net.openhft.chronicle.examples;

import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.VanillaChronicle;
import net.openhft.chronicle.VanillaChronicleConfig;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.Date;

public class WriteReadDatedMessages2Main {
    public static void main(String... ignored) throws IOException {
        String basePath = "/tmp/index";
        VanillaChronicleConfig config = new VanillaChronicleConfig();
        System.out.println("cycleFormat " + config.cycleFormat());
        System.out.println("cycleLength " + config.cycleLength());
        System.out.println("dataBlockSize " + config.dataBlockSize());
        System.out.println("defaultMessageSize " + config.defaultMessageSize());
        System.out.println("entriesPerCycle " + config.entriesPerCycle());
        System.out.println("indexBlockSize " + config.indexBlockSize());
        System.out.println("synchronous " + config.synchronous());
        System.out.println("MIN_CYCLE_LENGTH "
                + VanillaChronicleConfig.MIN_CYCLE_LENGTH);
        VanillaChronicle chronicle = new VanillaChronicle(basePath);
        int messages = 100000000;// 1000 * 50000;
        chronicle.clear();
        long start = System.nanoTime();
        VanillaChronicle.VanillaAppender appender;
        int i = 0;
        appender = chronicle.createAppender();
        String msg = "writer1 " + new Date();
        // count of records to be written in this index
        appender.startExcerpt();
        appender.writeInt(messages);
        appender.finish();
        for (i = 1; i <= messages; i++) {
            appender.startExcerpt();
            appender.writeInt(i);
            appender.writeLong(System.currentTimeMillis());
            appender.writeUTFΔ(msg);
            appender.finish();
        }

        System.out.println(chronicle.lastWrittenIndex());
        System.out.printf("After %,d, the chronicle took %.3f seconds%n", i,
                (System.nanoTime() - start) / 1e9);
        System.out.println("writing done");
        ExcerptTailer tailer;
        tailer = chronicle.createTailer();
        StringBuilder msgBuffer = new StringBuilder();
        start = System.nanoTime();
        int counter = 0;
        while (tailer.nextIndex()) {
            // read how much is written in this
            if (counter == 0) {
                counter = (tailer.readInt());
                tailer.finish();
                continue;
            }
            int readInt = tailer.readInt();
            tailer.readLong();
            tailer.readUTFΔ(msgBuffer);
            tailer.finish();
            if (readInt == counter) {
                // reading complete
                // do something
            }
        }
        long time = System.nanoTime() - start;
        // why cannot I clear the chronicle, I get
        // net.openhft.lang.io.IOTools deleteDir
        // unable to delete E:\~4V\chronicle-data\index\20140529\data-5628-8
        // chronicle.close();
        // chronicle.clear();

        System.out.printf("After %,d, the chronicle, took %.3f seconds%n",
                messages, (time / 1e9));
        // I run in Windows
        /**System.out
         .printf("After %,d, the chronicle uses %s KB, took %.3f seconds%n",
         messages, run("du", "-s", basePath).split("\\s")[0],
         time / 1e9);*/
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