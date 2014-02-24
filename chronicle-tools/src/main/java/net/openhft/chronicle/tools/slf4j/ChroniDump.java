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

import net.openhft.chronicle.sandbox.BytesProcessor;
import net.openhft.chronicle.sandbox.VanillaChronicle;
import net.openhft.chronicle.sandbox.tools.ChronicleProcessor;
import net.openhft.chronicle.slf4j.ChronicleLoggingConfig;
import net.openhft.lang.io.Bytes;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 *
 */
public class ChroniDump {
    private static final DateFormat DF =
        new SimpleDateFormat(ChronicleLoggingConfig.DEFAULT_DATE_FORMAT);

    // *************************************************************************
    //
    // *************************************************************************

    private static final BytesProcessor HEXDUMP = new BytesProcessor() {
        @Override
        public void process(Bytes bytes) {
        }
    };

    // *************************************************************************
    //
    // *************************************************************************

    public static void main(String[] args) {
        try {
            if(args.length == 1) {
                ChronicleProcessor cp = new ChronicleProcessor(
                    new VanillaChronicle(args[args.length - 1]),HEXDUMP);

                cp.run(false);
                cp.close();

            } else {
                System.err.format("\nUsage: ChroniDump [-t] path");
            }
        } catch(Exception e) {
            e.printStackTrace(System.err);
        }
    }
}
