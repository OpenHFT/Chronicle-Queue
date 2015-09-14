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
import org.jetbrains.annotations.NotNull;

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

    public static <T extends WriteMarshallable> T writeMeta(
        @NotNull WireOut wireOut,
        @NotNull T writer) {

        WireInternal.writeData(wireOut, true, false, writer);

        return writer;
    }

    public static <T extends ReadMarshallable> T readData(
            @NotNull WireIn wireIn,
            @NotNull T reader) {

        WireInternal.readData(wireIn, null, reader);

        return reader;
    }

    public static <T extends ReadMarshallable> T readMeta(
        @NotNull WireIn wireIn,
        @NotNull T reader) {

        WireInternal.readData(wireIn, reader, null);

        return reader;
    }
}
