/*
 * Copyright 2015 Higher Frequency Trading
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

package net.openhft.chronicle.sandbox.replay;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.lang.model.DataValueClasses;

import java.io.IOException;

public class ReplayData {
    /*
    On an i7-3970X prints.

    999999 : TestData{ age= 999999, importance= 0.0999999, timestamp= 1421790885572, name= Name999999 }
    1999999 : TestData{ age= 1999999, importance= 0.1999999, timestamp= 1421790885701, name= Name1999999 }
    2999999 : TestData{ age= 2999999, importance= 0.2999999, timestamp= 1421790885822, name= Name2999999 }
    4000000 : TestData{ age= 3999999, importance= 0.3999999, timestamp= 1421790885964, name= Name3999999 }
    5000000 : TestData{ age= 4999999, importance= 0.4999999, timestamp= 1421790886091, name= Name4999999 }
    6000000 : TestData{ age= 5999999, importance= 0.5999999, timestamp= 1421790886228, name= Name5999999 }
    7000000 : TestData{ age= 6999999, importance= 0.6999999, timestamp= 1421790886356, name= Name6999999 }
    8000001 : TestData{ age= 7999999, importance= 0.7999999, timestamp= 1421790886515, name= Name7999999 }
    9000001 : TestData{ age= 8999999, importance= 0.8999999, timestamp= 1421790886639, name= Name8999999 }
    10000001 : TestData{ age= 9999999, importance= 0.9999999, timestamp= 1421790886763, name= Name9999999 }
    Took 1.178 seconds to read 10,000,000 records
    */
    public static void main(String[] args) throws IOException {
        TestData td = DataValueClasses.newInstance(TestData.class);
        String path = OS.getTarget()+"/test"+ Time.uniqueId();

        long start = System.nanoTime(), count = 0;
        try (Chronicle chronicle = ChronicleQueueBuilder.indexed(path).build()) {
            ExcerptTailer tailer = chronicle.createTailer();
            while (tailer.nextIndex()) {
                td.readMarshallable(tailer);
                tailer.finish();
                if (td.getAge() != count) {
                    System.err.println(count + ":" + td);
                    break;
                }
                count++;
                if (count % 1000000 == 0)
                    System.out.println(tailer.index() + " : " + td);
            }
        }
        System.out.printf("Took %.3f seconds to read %,d records%n",
                (System.nanoTime() - start) / 1e9, count);
    }
}
