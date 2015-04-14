/*
 * Copyright 2015 Higher Frequency Trading
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

package vanilla.java.proxy;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ExcerptCommon;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;

import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * Created by mat on 10/02/2015.
 */
public class MetaData implements BytesMarshallable {
    static final ThreadLocal<MetaData> META_DATA_THREAD_LOCAL = new ThreadLocal<MetaData>() {
        @Override
        protected MetaData initialValue() {
            return new MetaData();
        }
    };
    int timeCount = 0;
    long[] times = new long[20];
    int sourceCount = 0;
    byte[] sourceIds = new byte[10];
    long[] sources = new long[10];

    static final Map<Chronicle, Byte> CHRONICLE_TO_ID =
            Collections.synchronizedMap(new WeakHashMap<Chronicle, Byte>());

    public static MetaData get() {
        return META_DATA_THREAD_LOCAL.get();
    }

    public static void setId(Chronicle c, byte id) {
        CHRONICLE_TO_ID.put(c, id);
    }

    @Override
    public void readMarshallable(Bytes bytes) throws IllegalStateException {
        timeCount = (int) bytes.readStopBit();
        for (int i = 0; i < timeCount; i++) {
            times[i] = bytes.readLong();
        }
        times[timeCount++] = System.nanoTime();
        sourceCount = (int) bytes.readStopBit();
        if (bytes instanceof ExcerptCommon) {
            ExcerptCommon excerpt = (ExcerptCommon) bytes;
            Byte id = CHRONICLE_TO_ID.get(excerpt.chronicle());
            sourceIds[sourceCount] = id == null ? (byte) -1 : id;
            sources[sourceCount++] = excerpt.index();
        }
    }

    @Override
    public void writeMarshallable(Bytes bytes) {
        bytes.writeStopBit(timeCount + 1);
        for (int i = 0; i < timeCount; i++) {
            bytes.writeLong(times[i]);
        }
        bytes.writeLong(System.nanoTime());

        bytes.writeStopBit(sourceCount);
        bytes.write(sourceIds, 0, sourceCount);
        for (int i = 0; i < sourceCount; i++) {
            bytes.writeLong(sources[i]);
        }
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("MetaData times,");
        if (timeCount > 0)
            sb.append(times[0] / 1000);
        for (int i = 1; i < timeCount; i++)
            sb.append(",").append((times[i] - times[i - 1]) / 1000);
        sb.append(", sources");
        for (int i = 0; i < sourceCount; i++) {
            sb.append(", ").append(sourceIds[i])
                    .append(",").append(Long.toHexString(sources[i]));
        }
        return sb.toString();
    }
}