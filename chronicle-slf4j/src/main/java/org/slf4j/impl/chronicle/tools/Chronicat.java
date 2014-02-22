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
package org.slf4j.impl.chronicle.tools;

import net.openhft.chronicle.sandbox.VanillaChronicle;
import net.openhft.chronicle.sandbox.tools.BytesProcessor;
import net.openhft.chronicle.sandbox.tools.ChronicleProcessor;
import net.openhft.lang.io.Bytes;
import org.slf4j.impl.chronicle.ChronicleLoggingConfig;
import org.slf4j.impl.chronicle.ChronicleLoggingHelper;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 */
public class Chronicat {
    private static final DateFormat DF =
        new SimpleDateFormat(ChronicleLoggingConfig.DEFAULT_DATE_FORMAT);

    // *************************************************************************
    // Processors
    // *************************************************************************

    private static final BytesProcessor BINARY = new BytesProcessor() {
        @Override
        public void process(Bytes bytes) {

            //TODO: date format
            Date   ts    = new Date(bytes.readLong());
            String level = ChronicleLoggingHelper.levelToString(bytes.readByte());
            String name  = bytes.readEnum(String.class);
            String msg   = bytes.readEnum(String.class);

            System.out.format("%s|%s|%s|%s\n",DF.format(ts),level,name,msg);
        }
    };

    private static final BytesProcessor TEXT = new BytesProcessor() {
        @Override
        public void process(Bytes bytes) {
            //TODO: implement
        }
    };

    // *************************************************************************
    //
    // *************************************************************************

    public static void main(String[] args) {
        try {
            boolean binary = true;
            String path = null;

            //TODO add more options
            for(int i=0;i<args.length - 1;i++) {
                if("-t".equals(args[i])) {
                    binary = false;
                }
            }

            if(args.length >= 1) {
                ChronicleProcessor cp = new ChronicleProcessor(
                    new VanillaChronicle(args[args.length -1]),
                    binary ? BINARY : TEXT);

                cp.run();
                cp.close();

            } else {
                System.err.format("\nUsage: Chronicat [-t] path");
                System.err.format("\n  -t text chroncile, default binary");
            }
        } catch(Exception e) {
            e.printStackTrace(System.err);
        }
    }
}
