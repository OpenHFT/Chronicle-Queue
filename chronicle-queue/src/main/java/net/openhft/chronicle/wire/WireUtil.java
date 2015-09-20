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
import net.openhft.chronicle.core.annotation.ForceInline;
import net.openhft.chronicle.core.pool.StringBuilderPool;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Function;

//TODO: workaround for protected access to WireInternal
public class WireUtil {
    public static final Logger LOGGER = LoggerFactory.getLogger(WireUtil.class);
    public static final StringBuilderPool SBP = new StringBuilderPool();

    public static final int LENGTH_MASK    = Wires.LENGTH_MASK;
    public static final int NOT_READY      = Wires.NOT_READY;
    public static final int META_DATA      = Wires.META_DATA;
    public static final int UNKNOWN_LENGTH = Wires.UNKNOWN_LENGTH;
    public static final int MAX_LENGTH     = LENGTH_MASK;
    public static final int FREE           = 0x0;
    public static final int BUILDING       = WireUtil.NOT_READY | WireUtil.UNKNOWN_LENGTH;
    public static final int NO_DATA        = 0;
    public static final int NO_INDEX       = -1;

    public static final long SPB_HEADER_BYTE      = 0;
    public static final long SPB_HEADER_BYTE_SIZE = 8;
    public static final long SPB_HEADER_UNSET     = 0x0;
    public static final long SPB_HEADER_BUILDING  = 0x1;
    public static final long SPB_HEADER_BUILT     = WireUtil.asLong("QUEUE400");
    public static final long SPB_DATA_HEADER_SIZE = 4;

    public static class WireBounds {
        public long lower = NO_DATA;
        public long upper = NO_DATA;
    }

    // *************************************************************************
    // MISC
    // *************************************************************************

    public static String hostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            try {
                return Files.readAllLines(Paths.get("etc", "hostname")).get(0);
            } catch (Exception e2) {
                return "localhost";
            }
        }
    }

    public static long asLong(@NotNull String str) {
        return ByteBuffer.wrap(str.getBytes(StandardCharsets.ISO_8859_1))
            .order(ByteOrder.nativeOrder())
            .getLong();
    }

    // *************************************************************************
    // WIRE
    // *************************************************************************

    public static boolean isKnownLength(int len) {
        return (len & (Wires.META_DATA | Wires.LENGTH_MASK)) != Wires.UNKNOWN_LENGTH;
    }

    public static boolean isData(int len) {
        return len != 0 && Wires.isData(len);
    }

    public static final Function<Bytes,Wire> wireSupplierFor(WireType type) {
        switch (type) {
            case BINARY:
                return BinaryWire::new;
            case TEXT:
                return TextWire::new;
            case RAW:
                return RawWire::new;
        }

        throw new IllegalArgumentException("Unknown WireType (" + type + ")");
    }

    @ForceInline
    public static <T extends ReadMarshallable> long readData(
            @NotNull WireIn wireIn,
            @NotNull T reader) {

        // We assume that check on data readiness and type has been done by the
        // caller
        WireInternal.rawReadData(wireIn, reader);

        return wireIn.bytes().readPosition();
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

        boolean result = WireInternal.readData(wireIn, reader, null);
        if(result) {
            return wireIn.bytes().readPosition();
        }

        return NO_DATA;
    }
}
