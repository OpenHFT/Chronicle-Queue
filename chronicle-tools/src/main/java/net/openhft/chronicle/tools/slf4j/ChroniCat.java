/*
 * Copyright 2014 Peter Lawrey
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
package net.openhft.chronicle.tools.slf4j;

import net.openhft.chronicle.sandbox.VanillaChronicle;
import net.openhft.chronicle.sandbox.tools.ChronicleProcessor;
import net.openhft.chronicle.slf4j.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 */
public class ChroniCat {
    private static final DateFormat DF =
        new SimpleDateFormat(ChronicleLoggingConfig.DEFAULT_DATE_FORMAT);

    // *************************************************************************
    //
    // *************************************************************************

    private static final ChronicleLogReader BINARY = new AbstractBinaryChronicleLogReader() {
        @Override
        public void read(Date timestamp, int level, String threadName, String name, String message, Throwable t) {
            System.out.format("%s|%s|%s|%s|%s\n",
                DF.format(timestamp),
                ChronicleLoggingHelper.levelToString(level),
                threadName,
                name,
                message);
        }
    };

    private static final ChronicleLogReader TEXT = new AbstractTextChronicleLogReader() {
        @Override
        public void read(Date timestamp, int level, String threadName, String name, String message, Throwable t) {
            System.out.format("%s|%s|%s|%s|%s\n",
                DF.format(timestamp),
                ChronicleLoggingHelper.levelToString(level),
                threadName,
                name,
                message);
        }
    };

    // *************************************************************************
    //
    // *************************************************************************

    public static void main(String[] args) {
        try {
            boolean binary = true;

            //TODO add more options
            for(int i=0;i<args.length - 1;i++) {
                if("-t".equals(args[i])) {
                    binary = false;
                }
            }

            if(args.length >= 1) {
                ChronicleProcessor cp = new ChronicleProcessor(
                    new VanillaChronicle(args[args.length - 1]),
                    binary ? BINARY : TEXT);

                cp.run(false);
                cp.close();

            } else {
                System.err.format("\nUsage: ChroniCat [-t] path");
                System.err.format("\n  -t = text chronicle, default binary");
            }
        } catch(Exception e) {
            e.printStackTrace(System.err);
        }
    }
}
