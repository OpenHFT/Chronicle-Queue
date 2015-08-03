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
 */
package net.openhft.chronicle.tcp.network;

import net.openhft.lang.io.Bytes;

/**
 * Based on https://github.com/OpenHFT/Chronicle-Network/blob/master/src/main/java/net/openhft/chronicle/network/api/TcpHandler.java
 *
 * for ease of eventual alignment
 */
public interface TcpHandler {

    /**
     * This signature has been modified from:
     *   https://github.com/OpenHFT/Chronicle-Network/blob/master/src/main/java/net/openhft/chronicle/network/api/TcpHandler.java
     *
     * to include a boolean as the return type.  There
     * are cases whereby a handler may cache the upstream data (i.e. SSL) for subsequent
     * reads by downstream TcpHandlers.  In this scenario, upstream data (in, out params) would not
     * be read - however the handler should still be considered busy as it's internal buffer has
     * been consumed from.
     *
     * @param in the bytes to read from
     * @param out the bytes written to
     * @param sessionDetailsProvider tcp session details
     * @return true for busy, false otherwise
     */
    boolean process(Bytes in, Bytes out, SessionDetailsProvider sessionDetailsProvider);

    // Still only Java 7 support, can't use default methods
    void onEndOfConnection(SessionDetailsProvider sessionDetailsProvider);

    class BusyChecker {
        private long inPos, outPos;

        public void mark(Bytes in, Bytes out) {
            inPos = in.position();
            outPos = out.position();
        }

        public boolean busy(Bytes in, Bytes out) {
            return in.position() > inPos || out.position() > outPos;
        }
    }
}
