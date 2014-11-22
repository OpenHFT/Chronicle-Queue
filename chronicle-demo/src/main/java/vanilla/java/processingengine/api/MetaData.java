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

package vanilla.java.processingengine.api;

import net.openhft.chronicle.ExcerptCommon;
import net.openhft.lang.io.RandomDataOutput;
import net.openhft.lang.model.constraints.NotNull;

/**
 * @author peter.lawrey
 */
public class MetaData {
    private boolean targetReader;

    public int sourceId;
    private long excerptId;
    private long writeTimestampMillis;
    public int inWriteTimestamp;
    private int inReadTimestamp;
    private int outWriteTimestamp;
    public int outReadTimestamp;

    public MetaData(boolean targetReader) {
        this.targetReader = targetReader;
    }

    private static int fastTime() {
        return (int) System.nanoTime();
    }

    public static void writeForGateway(@NotNull RandomDataOutput out) {
        out.writeLong(System.currentTimeMillis());
        out.writeInt(fastTime());
        out.writeInt(0);
    }

    public void readFromGateway(@NotNull ExcerptCommon in) {
        excerptId = in.index();
        writeTimestampMillis = in.readLong();
        inWriteTimestamp = in.readInt();
        inReadTimestamp = in.readInt();
        if (inReadTimestamp == 0 && targetReader)
            in.writeInt(in.position() - 4,
                    inReadTimestamp = fastTime());
    }

    public void writeForEngine(@NotNull RandomDataOutput out) {
        out.writeInt(sourceId);
        out.writeLong(excerptId);
        out.writeLong(writeTimestampMillis);
        out.writeInt(inWriteTimestamp);
        out.writeInt(inReadTimestamp);
        out.writeInt(fastTime());
        out.writeInt(0);
    }

    public void readFromEngine(@NotNull ExcerptCommon in, int sourceId) {
        this.sourceId = in.readInt();
        excerptId = in.readLong();
        targetReader = sourceId == this.sourceId;
        writeTimestampMillis = in.readLong();
        inWriteTimestamp = in.readInt();
        inReadTimestamp = in.readInt();
        outWriteTimestamp = in.readInt();
        outReadTimestamp = in.readInt();
        if (outReadTimestamp == 0 && targetReader)
            in.writeInt(in.position() - 4,
                    outReadTimestamp = fastTime());
    }
}
