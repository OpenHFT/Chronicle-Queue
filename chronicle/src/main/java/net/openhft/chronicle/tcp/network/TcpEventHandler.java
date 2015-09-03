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

import net.openhft.chronicle.tcp.TcpConnectionHandler;
import net.openhft.chronicle.tcp.TcpConnectionListener;
import net.openhft.lang.Maths;
import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.SocketChannel;

import static net.openhft.lang.io.ByteBufferBytes.wrap;

public class TcpEventHandler {
    public static final int CAPACITY = 1 << 23;
    private static final int TOO_MUCH_TO_WRITE = 32 << 10;
    private static final Logger log = LoggerFactory.getLogger(TcpEventHandler.class);

    private final SocketChannel sc;
    private final TcpHandler handler;
    private final ByteBuffer inBB;
    private final ByteBuffer outBB;
    private final Bytes inBBB;
    private final Bytes outBBB;
    private final SessionDetailsProvider sessionDetails;
    private final TcpConnectionListener tcpConnectionListener;

    public TcpEventHandler(
            @NotNull SocketChannel sc,
            TcpHandler handler,
            SessionDetailsProvider sessionDetails,
            TcpConnectionListener tcpConnectionListener,
            int sendCapacity,
            int receiveCapacity) throws IOException {
        this.tcpConnectionListener = tcpConnectionListener;
        this.inBB = ByteBuffer.allocateDirect(receiveCapacity > 0 ? receiveCapacity : CAPACITY);
        this.outBB = ByteBuffer.allocateDirect(sendCapacity > 0 ? sendCapacity : CAPACITY);

        this.sc = sc;
        sc.configureBlocking(false);
        sc.socket().setTcpNoDelay(true);
        sc.socket().setReceiveBufferSize(inBB.capacity());
        sc.socket().setSendBufferSize(outBB.capacity());
        sc.socket().setSoTimeout(0);
        sc.socket().setSoLinger(false, 0);

        this.handler = handler;
        this.sessionDetails = sessionDetails;
        // allow these to be used by another thread.
        // todo check that this can be commented out
        // inBBB.clearThreadAssociation();
        //  outBBB.clearThreadAssociation();

        this.inBBB = wrap(inBB.slice());
        this.outBBB = wrap(outBB.slice());
        // must be set after we take a slice();
        limitSocketOutput(0);
        limitApplicationInput(0);
        positionApplicationOutput(0);
    }

    public TcpEventHandler(@NotNull SocketChannel sc, TcpHandler handler, SessionDetailsProvider sessionDetails) throws IOException {
        this(sc, handler, sessionDetails, new TcpConnectionHandler(), CAPACITY, CAPACITY);
    }

    public boolean action() throws InvalidEventHandlerException {
        final SocketChannel sc = this.sc;
        if (!sc.isOpen()) {
            handleClosedSocket();
        }

        try {
            if (readSocket(sc)) return invokeHandler();
        } catch (IOException e) {
            handleIOE(e);
        }

        return false;
    }

    private boolean readSocket(final SocketChannel sc) throws IOException {
        final ByteBuffer inBB = this.inBB;
        int read = inBB.remaining() > 0 ? sc.read(inBB) : 1;
        if (read < 0) {
            closeSC();
            tcpConnectionListener.onDisconnect(sc, "Failed to read from socket, closing.");
            return false;
        }
        limitApplicationInput(inBB.position());
        return true;
    }

    private void handleClosedSocket() throws InvalidEventHandlerException {
        handler.onEndOfConnection(sessionDetails);
        throw new InvalidEventHandlerException("Cannot process, socket chanel is not open.");
    }

    protected boolean invokeHandler() throws IOException {
        final Bytes inBBB = this.inBBB;
        final Bytes outBBB = this.outBBB;
        final long preProcessApplicationInputPos = inBBB.position();
        boolean busy = handler.process(inBBB, outBBB, sessionDetails);
        busy |= postHandlerProcess(inBBB, preProcessApplicationInputPos, outBBB);
        return busy;
    }

    private boolean postHandlerProcess(Bytes inBBB, long preProcessApplicationInputPos, Bytes outBBB) throws IOException {
        boolean busy = writeHandlerOutput(outBBB);
        busy |= compactInput(inBBB, preProcessApplicationInputPos);
        return busy;
    }

    private boolean compactInput(Bytes inBBB, long preProcessApplicationInputPos) {
        final long inBBBPos = inBBB.position();
        if (preProcessApplicationInputPos == inBBBPos) {
            return false;
        }
        if (shouldCompact(inBBBPos)) {
            performCompaction((int) inBBBPos);
        }
        return true;
    }

    private void positionApplicationOutput(int position) {
        outBBB.position(position);
    }

    private void limitApplicationInput(int limit) {
        inBBB.limit(limit);
    }

    private void performCompaction(int inBBBPos) {
        alignInputBuffers(inBBBPos);
        compactSocketInput();
        resetApplicationInput();
        limitApplicationInput(inBB.position());
    }

    private boolean shouldCompact(long inBBBPos) {
        return (inBBBPos << 1) > inBB.limit();
    }

    private void resetApplicationInput() {
        inBBB.position(0);
        limitApplicationInput(inBB.position());
    }

    private void compactSocketInput() {
        inBB.compact();
    }

    private void alignInputBuffers(int inBBBPos) {
        // inBB.position((int) inBBB.readPosition());
        inBB.position(inBBBPos);
        // inBB.limit((int) inBBB.readLimit());
        inBB.limit((int) inBBB.limit());
    }

    private boolean writeHandlerOutput(Bytes outBBB) throws IOException {
        // did it write something?
        // if (outBBB.writePosition() > outBB.limit() || outBBB.writePosition() >= 4) {
        final long outBBBPos = outBBB.position();
        if (handlerWroteSomething(outBBBPos)) {
            // outBB.limit(Maths.toInt32(outBBB.writePosition()));
            limitSocketOutput(outBBBPos);
            tryWrite(outBBB);
            return true;
        }
        return false;
    }

    private void limitSocketOutput(long newLimit) {
        outBB.limit(Maths.toInt(newLimit, "Int %d out of range"));
    }

    private boolean handlerWroteSomething(final long outBBBPos) {
        return outBBBPos > outBB.limit() || outBBBPos >= 4;
    }

    boolean tryWrite(Bytes outBBB) throws IOException {
        int wrote = sc.write(outBB);
        if (wrote < 0) {
            closeSC();
            tcpConnectionListener.onDisconnect(sc, "Failed to write to socket, closing.");
        } else if (wrote > 0) {
            outBB.compact().flip();
            positionApplicationOutput(outBB.limit());
            // outBBB.writeLimit(outBB.capacity());
            outBBB.limit(outBB.capacity());
            return true;
        }
        return false;
    }

    void handleIOE(@NotNull IOException e) {
        if (!(e instanceof ClosedByInterruptException)) {
            if (remoteDisconnection(e)) {
                tcpConnectionListener.onDisconnect(sc, e.getMessage());
            } else {
                tcpConnectionListener.onError(sc, e);
            }
        }
        closeSC();
    }

    private void closeSC() {
        try {
            sc.close();
        } catch (IOException ignored) {
        }
    }

    private boolean remoteDisconnection(IOException ioe) {
        String msg = ioe.getMessage();
        return (msg != null && (
                    msg.contains("reset by peer") ||
                    msg.contains("Broken pipe") ||
                    msg.contains("was aborted by")));

    }
}
