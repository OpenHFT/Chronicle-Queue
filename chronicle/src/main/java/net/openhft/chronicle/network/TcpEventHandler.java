package net.openhft.chronicle.network;

import net.openhft.lang.Maths;
import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.SocketChannel;

import static net.openhft.lang.io.ByteBufferBytes.wrap;

public class TcpEventHandler {
    public static final ThreadLocal<Boolean> LOG = new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
            return Boolean.FALSE;
        }
    };

    public static final int CAPACITY = 1 << 23;
    private static final int TOO_MUCH_TO_WRITE = 32 << 10;

    private final SocketChannel sc;
    private final TcpHandler handler;
    private final ByteBuffer inBB = ByteBuffer.allocateDirect(CAPACITY);
    private final ByteBuffer outBB = ByteBuffer.allocateDirect(CAPACITY);
    private final Bytes inBBB;
    private final Bytes outBBB;
    private final SessionDetailsProvider sessionDetails;

//    public TcpEventHandler(@NotNull SocketChannel sc, TcpHandler handler, SessionDetails sessionDetails, boolean unchecked) throws IOException {
    public TcpEventHandler(@NotNull SocketChannel sc, TcpHandler handler, SessionDetailsProvider sessionDetails) throws IOException {
        this.sc = sc;
        sc.configureBlocking(false);
        sc.socket().setTcpNoDelay(true);
        sc.socket().setReceiveBufferSize(CAPACITY);
        sc.socket().setSendBufferSize(CAPACITY);

        this.handler = handler;
        this.sessionDetails = sessionDetails;
        // allow these to be used by another thread.
        // todo check that this can be commented out
        // inBBB.clearThreadAssociation();
        //  outBBB.clearThreadAssociation();

        inBBB = wrap(inBB.slice());
        outBBB = wrap(outBB.slice());
        // must be set after we take a slice();
        outBB.limit(0);
    }

    public boolean action() throws InvalidEventHandlerException {
        if (!sc.isOpen()) {
            handler.onEndOfConnection();
            throw new InvalidEventHandlerException("Cannot process, socket chanel is not open.");
        }

        try {
            int read = inBB.remaining() > 0 ? sc.read(inBB) : 1;
            if (read < 0) {
                closeSC();

            } else if (read >= 0) {
                // inBB.position() where the data has been read() up to.
                return invokeHandler();
            }
        } catch (IOException e) {
            handleIOE(e);
        }

        return false;
    }

    boolean invokeHandler() throws IOException {
        long start = System.nanoTime();
        boolean busy = false;
        // inBBB.readLimit(inBB.position());
        inBBB.limit(inBB.position());
        // outBBB.writePosition(outBB.limit());
        outBBB.position(outBB.limit());
        handler.process(inBBB, outBBB, sessionDetails);

        // did it write something?
        // if (outBBB.writePosition() > outBB.limit() || outBBB.writePosition() >= 4) {
        if (outBBB.position() > outBB.limit() || outBBB.position() >= 4) {
            // outBB.limit(Maths.toInt32(outBBB.writePosition()));
            outBB.limit(Maths.toInt(outBBB.position(), "Int %d out of range"));
            tryWrite();
            busy = true;
        }

        long compact = System.nanoTime();
        long inBBCompactStart = -1, inBBCompactEnd = -1;
        // TODO Optimise.
        // if it read some data compact();
        // if (inBBB.readPosition() > 0) {
        if (inBBB.position() > (inBB.limit() / 2)) {
            // inBB.position((int) inBBB.readPosition());
            inBB.position((int) inBBB.position());
            // inBB.limit((int) inBBB.readLimit());
            inBB.limit((int) inBBB.limit());
            inBBCompactStart = System.nanoTime();
            inBB.compact();
            inBBCompactEnd = System.nanoTime();
            inBBB.position(0);
            // inBBB.readLimit(inBB.position());
            inBBB.limit(inBB.position());
            busy = true;
        }
        if (LOG.get()) {
            long now = System.nanoTime();
            System.out.println("invokeHandler; time; compact(us): " + ((now - compact) / 1000.0) + ", inBB.compact(us): " + ((inBBCompactEnd - inBBCompactStart) / 1000.0) + ", total(us): " + ((now - start) / 1000.0));
        }
        return busy;
    }

    boolean tryWrite() throws IOException {
        int wrote = sc.write(outBB);
        if (wrote < 0) {
            closeSC();

        } else if (wrote > 0) {
            outBB.compact().flip();
            // outBBB.writePosition(outBB.limit());
            outBBB.position(outBB.limit());
            // outBBB.writeLimit(outBB.capacity());
            outBBB.limit(outBB.capacity());
            return true;
        }
        return false;
    }

    void handleIOE(@NotNull IOException e) {
        if (!(e instanceof ClosedByInterruptException))
            e.printStackTrace();
        closeSC();
    }

    private void closeSC() {
        try {
            sc.close();
        } catch (IOException ignored) {
        }
    }
}
