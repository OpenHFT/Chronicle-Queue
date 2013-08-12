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

import net.openhft.chronicle.Excerpt;

/**
 * @author peter.lawrey
 */
public class MetaData {
    boolean targetReader;

    public int sourceId;
    public long excerptId;
    public long writeTimestampMillis;
    public long inWriteTimestamp7; // tenths of a micro-second
    public long inReadTimestamp7Delta; // tenths of a micro-second
    public long outWriteTimestamp7Delta; // tenths of a micro-second
    public long outReadTimestamp7Delta; // tenths of a micro-second

    public MetaData(boolean targetReader) {
        this.targetReader = targetReader;
    }

    private static long fastTime() {
        return System.nanoTime() / 100;
    }

    public static void writeForGateway(Excerpt out) {
        out.writeLong(System.currentTimeMillis());
        out.writeLong(fastTime());
        out.writeInt(0);
    }

    public void readFromGateway(Excerpt in) {
        excerptId = in.index();
        writeTimestampMillis = in.readLong();
        inWriteTimestamp7 = in.readLong();
        inReadTimestamp7Delta = in.readUnsignedInt();
        if (inReadTimestamp7Delta == 0 && targetReader)
            in.writeUnsignedInt(in.position() - 4,
                    inReadTimestamp7Delta = fastTime() - inWriteTimestamp7);
    }

    public void writeForEngine(Excerpt out) {
        out.writeInt(sourceId);
        out.writeLong(excerptId);
        out.writeLong(writeTimestampMillis);
        out.writeLong(inWriteTimestamp7);
        out.writeUnsignedInt(inReadTimestamp7Delta);
        out.writeUnsignedInt(fastTime() - inWriteTimestamp7);
        out.writeUnsignedInt(0L);
    }

    public void readFromEngine(Excerpt in, int sourceId) {
        this.sourceId = in.readInt();
        excerptId = in.readLong();
        targetReader = sourceId == this.sourceId;
        writeTimestampMillis = in.readLong();
        inWriteTimestamp7 = in.readLong();
        inReadTimestamp7Delta = in.readUnsignedInt();
        outWriteTimestamp7Delta = in.readUnsignedInt();
        outReadTimestamp7Delta = in.readUnsignedInt();
        if (outReadTimestamp7Delta == 0 && targetReader)
            in.writeUnsignedInt(in.position() - 4,
                    outReadTimestamp7Delta = fastTime() - inWriteTimestamp7);
    }
}
