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

package net.openhft.chronicle.examples;

import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.IndexedChronicle;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class WritingTextMain {
    public static void main(String[] args) throws IOException {
        String basePath = "/tmp/my.log";
        // clear the file first.
        for (String name : new String[]{basePath + ".data", basePath + ".index"}) {
            File file = new File(name);
            file.delete();
        }
        IndexedChronicle chronicle = new IndexedChronicle(basePath);

// write one object
        ExcerptAppender appender = chronicle.createAppender();
        appender.startExcerpt();
        appender.append("TestMessage\n");
        appender.finish();

        appender.startExcerpt();
        appender.append("Hello World\n");
        appender.finish();

        appender.startExcerpt();
        appender.append("Bye for now\n");
        appender.finish();

        chronicle.close();

        BufferedReader br = new BufferedReader(new FileReader(basePath + ".data"));
        for (int i = 0; i < 3; i++) {
            String line = br.readLine();
            System.out.println(line);
        }
        br.close();
    }
}
