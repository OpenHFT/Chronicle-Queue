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

package vanilla.java.processingengine.api;

import net.openhft.chronicle.ExcerptCommon;
import net.openhft.lang.io.RandomDataOutput;
import org.jetbrains.annotations.NotNull;

/**
 * @author peter.lawrey
 */
public class MetaData {
    boolean targetReader;

    public int sourceId;
    public long excerptId;
    public long writeTimestampMillis;
    public long inWriteTimestamp;
    public long inReadTimestamp;
    public long outWriteTimestamp;
    public long outReadTimestamp;

    public MetaData(boolean targetReader) {
        this.targetReader = targetReader;
    }

    private static long fastTime() {
        return System.nanoTime();
    }

    public static void writeForGateway(@NotNull RandomDataOutput out) {
        out.writeLong(System.currentTimeMillis());
        out.writeLong(fastTime());
        out.writeLong(0);
    }

    public void readFromGateway(@NotNull ExcerptCommon in) {
        excerptId = in.index();
        writeTimestampMillis = in.readLong();
        inWriteTimestamp = in.readLong();
        inReadTimestamp = in.readLong();
        if (inReadTimestamp == 0 && targetReader)
            in.writeLong(in.position() - 8,
                    inReadTimestamp = fastTime());
    }

    public void writeForEngine(@NotNull RandomDataOutput out) {
        out.writeInt(sourceId);
        out.writeLong(excerptId);
        out.writeLong(writeTimestampMillis);
        out.writeLong(inWriteTimestamp);
        out.writeLong(inReadTimestamp);
        out.writeLong(fastTime());
        out.writeLong(0L);
    }

    public void readFromEngine(@NotNull ExcerptCommon in, int sourceId) {
        this.sourceId = in.readInt();
        excerptId = in.readLong();
        targetReader = sourceId == this.sourceId;
        writeTimestampMillis = in.readLong();
        inWriteTimestamp = in.readLong();
        inReadTimestamp = in.readLong();
        outWriteTimestamp = in.readLong();
        outReadTimestamp = in.readLong();
        if (outReadTimestamp == 0 && targetReader)
            in.writeLong(in.position() - 8,
                    outReadTimestamp = fastTime());
    }
}
