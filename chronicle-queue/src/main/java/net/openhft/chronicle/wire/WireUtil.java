/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.openhft.chronicle.wire;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.annotation.ForceInline;
import net.openhft.chronicle.core.pool.StringBuilderPool;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

//TODO: workaround for protected access to WireInternal
public class WireUtil {
    public static final Logger LOGGER = LoggerFactory.getLogger(WireUtil.class);
    public static final StringBuilderPool SBP = new StringBuilderPool();

    public static final int LENGTH_MASK     = Wires.LENGTH_MASK;
    public static final int NOT_READY       = Wires.NOT_READY;
    public static final int UNKNOWN_LENGTH  = Wires.UNKNOWN_LENGTH;
    public static final int NOT_INITIALIZED = Wires.NOT_INITIALIZED;
    public static final int BUILDING        = WireUtil.NOT_READY | WireUtil.UNKNOWN_LENGTH;
    public static final int NO_DATA         = 0;
    public static final int NO_INDEX        = -1;

    public static final long HEADER_OFFSET = 0;
    public static final long SPB_DATA_HEADER_SIZE = 4;

    public static class WireBounds {
        public long lower = NO_DATA;
        public long upper = NO_DATA;
    }

    // *************************************************************************
    // WIRE
    // *************************************************************************

    @ForceInline
    public static <T extends ReadMarshallable> long readData(
            @NotNull WireIn wireIn,
            @NotNull T reader) {

        // We assume that check on data readiness and type has been done by the
        // caller
        return rawRead(wireIn, reader);
    }

    @ForceInline
    public static <T extends WriteMarshallable> long writeData(
            @NotNull WireOut wireOut,
            @NotNull T writer) {

        WireInternal.writeData(wireOut, false, false, writer);

        return wireOut.bytes().writePosition();
    }

    @ForceInline
    public static <T extends WriteMarshallable> long writeMeta(
            @NotNull WireOut wireOut,
            @NotNull T writer) {

        WireInternal.writeData(wireOut, true, false, writer);

        return wireOut.bytes().writePosition();
    }

    @ForceInline
    public static <T extends ReadMarshallable> long readMeta(
            @NotNull WireIn wireIn,
            @NotNull T reader) {

        // We assume that check on meta-data readiness and type has been done by
        // the caller
        return rawRead(wireIn, reader);
    }

    @ForceInline
    static long rawRead(@NotNull WireIn wireIn, @NotNull ReadMarshallable dataConsumer) {

        final Bytes<?> bytes = wireIn.bytes();
        final int header = bytes.readVolatileInt(bytes.readPosition());
        final int len = Wires.lengthOf(header);

        bytes.readSkip(4);

        final long limit0 = bytes.readLimit();
        final long limit = bytes.readPosition() + (long) len;
        try {
            bytes.readLimit(limit);
            dataConsumer.readMarshallable(wireIn);
        } finally {
            bytes.readLimit(limit0);
            bytes.readPosition(limit);
        }

        return bytes.readPosition();
    }

    /**
     * Wait for a Wire to be ready.
     *
     * If it exceed the number of attempts defined by loops each with a timeout
     * of sleep) it will throw an AssertionError.
     *
     * @param store
     * @param position
     * @param loops
     * @param sleep
     * @throws IOException
     */
    public static void waitForWireToBeReady(
        @NotNull BytesStore store, long position, int loops, int sleep) throws IOException {

        for (int i = loops; i >= 0; i--) {
            if(!Wires.isReady(store.readVolatileInt(position))) {
                Jvm.pause(sleep);
            } else {
                return;
            }
        }

        throw new AssertionError("Timeout waiting for a wire to be ready");
    }
}
