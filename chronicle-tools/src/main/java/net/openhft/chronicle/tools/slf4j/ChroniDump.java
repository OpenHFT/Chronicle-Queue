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
import net.openhft.lang.io.Bytes;

/**
 *
 */
public class ChroniDump {

    private final static char[] HEX_DIGITS = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
        'A', 'B', 'C', 'D', 'E', 'F'
    };

    // *************************************************************************
    //
    // *************************************************************************

    private static final BytesProcessor HEXDUMP = new BytesProcessor() {
        @Override
        public void process(Bytes bytes) {
            StringBuilder result = new StringBuilder();
            long size  = bytes.remaining();
            long chnks = size / 16;

            for(long i=0;i<chnks;i++) {
                for(int n=0;n<16;n++) {
                    //Read byte, no consume
                    byte b = bytes.readByte((chnks * 16) + n);

                    result.append(" ");
                    result.append(HEX_DIGITS[(b >>> 4) & 0x0F]);
                    result.append(HEX_DIGITS[b & 0x0F]);
                }

                result.append(" ==> ");

                for(int n=0;n<16;n++) {
                    //Read byte, consume
                    byte b = bytes.readByte();

                    if(b > ' ' && b < '~') {
                        result.append((char)b);
                    } else {
                        result.append(".");
                    }
                }

                result.append('\n');
            }

            //TODO handle remaining data

            System.out.println(result.toString());
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
