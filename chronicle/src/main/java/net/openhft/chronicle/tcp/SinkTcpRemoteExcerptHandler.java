package net.openhft.chronicle.tcp;

import net.openhft.chronicle.network.SessionDetailsProvider;
import net.openhft.chronicle.network.TcpHandler;
import net.openhft.chronicle.network.TcpHandlingException;
import net.openhft.chronicle.tools.ResizableDirectByteBufferBytes;
import net.openhft.lang.io.Bytes;

/**
 * Created by lear on 15/07/2015.
 */
public class SinkTcpRemoteExcerptHandler implements TcpHandler {

    private boolean subscribed;

    private boolean subscriptionRequired;

    private long subscribedIndex;

    private ResizableDirectByteBufferBytes content;

    private long index = -1;

    private State state;

    @Override
    public void process(Bytes in, Bytes out, SessionDetailsProvider sessionDetailsProvider) {
        processIncoming(in);
        processOutgoing(out);
    }

    private void processOutgoing(Bytes out) {
        if (subscriptionRequired) {
            out.writeLong(ChronicleTcp.ACTION_SUBSCRIBE);
            out.writeLong(subscribedIndex);
            subscriptionRequired = false;
        }
    }

    private void processIncoming(Bytes in) {
        if (!subscribed) {
            processSubscription(in);
        } else {
            nextExcerpt(in);
        }
    }

    private void processSubscription(Bytes in) {
        if (in.remaining() >= ChronicleTcp.HEADER_SIZE) {
            long startPos = in.position();
            int receivedSize = in.readInt();
            long receivedIndex = in.readLong();

            switch (receivedSize) {
                case ChronicleTcp.SYNC_IDX_LEN:
                    if (subscribedIndex == ChronicleTcp.IDX_TO_START) {
                        state = receivedIndex == -1 ? State.INDEX_FOUND : State.INDEX_NOT_FOUND;
                        subscribed = true;
                        return;

                    } else if (subscribedIndex == ChronicleTcp.IDX_TO_END) {
                        nextExcerpt(in);
                        subscribed = true;
                        return;

                    } else if (subscribedIndex == receivedIndex) {
                        nextExcerpt(in);
                        subscribed = true;
                        return;
                    }

                case ChronicleTcp.IN_SYNC_LEN:
                case ChronicleTcp.PADDED_LEN:
                    state = State.INDEX_NOT_FOUND;
                    subscribed = true;
                    return;
            }

            // skip excerpt
            if (in.remaining() >= receivedSize) {
                long inLimit = in.limit();
                try {
                    in.limit(in.position() + receivedSize);
                    content.resetToSize(receivedSize).write(in);
                    content.flip();
                } finally {
                    in.limit(inLimit);
                }
                state = State.SEARCHING;
            } else {
                in.position(startPos);
                state = State.UNDERFLOW;
            }
        } else {
            state = State.UNDERFLOW;
        }
        subscribed = false;
    }

    private void nextExcerpt(Bytes in) {
        if (in.remaining() >= ChronicleTcp.HEADER_SIZE) {
            long startPos = in.position();
            int receivedSize = in.readInt();
            long receivedIndex = in.readLong();

            switch (receivedSize) {
                case ChronicleTcp.IN_SYNC_LEN:
                    state = State.INDEX_NOT_FOUND;
                    return;
                case ChronicleTcp.SYNC_IDX_LEN:
                case ChronicleTcp.PADDED_LEN:
                    nextExcerpt(in);
                    return;
            }

            if (receivedSize > 128 << 20 || receivedSize < 0) {
                throw new TcpHandlingException("Size was " + receivedSize);
            }

            if (in.remaining() >= receivedSize) {
                long inLimit = in.limit();
                try {
                    in.limit(in.position() + receivedSize);
                    content.resetToSize(receivedSize).write(in);
                    content.flip();
                } finally {
                    in.limit(inLimit);
                }

                index = receivedIndex;
                state = State.INDEX_FOUND;
            } else {
                in.position(startPos);
                state = State.UNDERFLOW;
            }
        } else {
            state = State.UNDERFLOW;
        }
    }

    @Override
    public void onEndOfConnection() {

    }

    public void subscribeTo(long index) {
        this.subscribedIndex = index;
        this.subscriptionRequired = true;
    }

    public void setContent(ResizableDirectByteBufferBytes content) {
        this.content = content;
    }

    public State getState() {
        return state;
    }

    public long index() {
        return index;
    }

    public enum State {
        INDEX_NOT_FOUND,
        INDEX_FOUND,
        SEARCHING,
        UNDERFLOW
    }
}
