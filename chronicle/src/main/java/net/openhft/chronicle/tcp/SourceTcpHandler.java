package net.openhft.chronicle.tcp;

import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.MappingFunction;
import net.openhft.chronicle.MappingProvider;
import net.openhft.chronicle.network.SessionDetailsProvider;
import net.openhft.chronicle.network.TcpHandler;
import net.openhft.chronicle.tools.ResizableDirectByteBufferBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.model.constraints.NotNull;
import net.openhft.lang.thread.Pauser;
import org.jetbrains.annotations.Nullable;

import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by lear on 10/07/15.
 */
public abstract class SourceTcpHandler implements TcpHandler {

    protected Pauser pauser;

    protected ExcerptTailer tailer;

    protected ExcerptAppender appender;

    protected SubscriptionListener subscriptionListener;

    private boolean subscribed;

    private long lastHeartbeat;

    protected int maxExcerptsPerMessage;

    private long heartbeatIntervalMillis;

    protected long index;

    protected ResizableDirectByteBufferBytes withMappedBuffer;

    public SourceTcpHandler() {
        this.withMappedBuffer = new ResizableDirectByteBufferBytes(1024);
    }

    public void setTailer(ExcerptTailer tailer) {
        this.tailer = tailer;
    }

    public void setPauser(Pauser pauser) {
        this.pauser = pauser;
    }

    public void setSubscriptionListener(SubscriptionListener subscriptionListener) {
        this.subscriptionListener = subscriptionListener;
    }

    public void setMaxExcerptsPerMessage(int maxExcerptsPerMessage) {
        this.maxExcerptsPerMessage = maxExcerptsPerMessage;
    }

    public void setHeartbeatIntervalMillis(long heartbeatIntervalMillis) {
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
    }

    @Override
    public void process(Bytes in, Bytes out, SessionDetailsProvider sessionDetailsProvider) {
        processIncoming(in, out, sessionDetailsProvider);
        processOutgoing(out, sessionDetailsProvider);
    }

    private void processIncoming(Bytes in, Bytes out, SessionDetailsProvider sessionDetailsProvider) {
        while (in.remaining() >= 8) {

            final long action = in.readLong();
            try {
                switch ((int) action) {
                    case (int) ChronicleTcp.ACTION_WITH_MAPPING:
                        if (!onMapping(in, sessionDetailsProvider)) {
                            unreadAction(in);
                            return;
                        }
                        break;
                    case (int) ChronicleTcp.ACTION_SUBSCRIBE:
                        if (!(subscribed = onSubscribe(in, out, sessionDetailsProvider))) {
                            unreadAction(in); // wind back the long we read
                            return;
                        }
                        break;
                    case (int) ChronicleTcp.ACTION_UNSUBSCRIBE:
                        if (!onUnsubscribe(in)) {
                            unreadAction(in);
                            return;
                        }
                        break;
                    case (int) ChronicleTcp.ACTION_QUERY:
                        if (!onQuery(in, out)) {
                            in.position(in.position() - 8);
                            return;
                        }
                        break;
                    case (int) ChronicleTcp.ACTION_SUBMIT:
                        if (!onSubmit(in, out, true)) {
                            in.position(in.position() - 8);
                            return;
                        }
                        break;
                    case (int) ChronicleTcp.ACTION_SUBMIT_NOACK:
                        if (!onSubmit(in, out, false)) {
                            in.position(in.position() - 8);
                            return;
                        }
                        break;
                    default:
                        throw new IOException("Unknown action received (" + action + ")");
                }
            } catch (IOException e) {
//                throw e;
            }
        }
    }

    private void unreadAction(Bytes in) {
        in.position(in.position() - 8);
    }

    private void processOutgoing(Bytes out, SessionDetailsProvider sessionDetailsProvider) {
        final long now = System.currentTimeMillis();

        if (subscribed && !write(out, sessionDetailsProvider)) {
            if (lastHeartbeat <= now) {
                writeSizeAndIndex(out, ChronicleTcp.IN_SYNC_LEN, ChronicleTcp.IDX_NONE);
            }
        }
    }

    protected abstract boolean write(Bytes out, SessionDetailsProvider sessionDetailsProvider);

    protected void writeSizeAndIndex(Bytes out, int size, long index) {
        out.writeInt(size);
        out.writeLong(index);
        setLastHeartbeat();
    }

    protected void setLastHeartbeat() {
        setLastHeartbeat(System.currentTimeMillis());
    }

    protected void setLastHeartbeat(long now) {
        this.lastHeartbeat = now + heartbeatIntervalMillis;
    }

    @Override
    public void onEndOfConnection() {

    }

    protected abstract boolean onSubmit(Bytes in, Bytes out, boolean ack) throws IOException;

    protected abstract boolean onSubscribe(Bytes in, Bytes out, SessionDetailsProvider sessionDetailsProvider);

    protected boolean onUnsubscribe(Bytes in) throws IOException {
        if (in.remaining() >= 8) {
            in.readLong(); // do nothing with the long
            subscriptionListener.unsubscribed();
            return true;
        }
        return false;
    }

    protected boolean onQuery(Bytes in, Bytes out) throws IOException {
        if (in.remaining() >= 8) {
            long index = in.readLong();
            if (tailer.index(index)) {
                final long now = System.currentTimeMillis();
                setLastHeartbeat(now);

                while (true) {
                    if (tailer.nextIndex()) {
                        writeSizeAndIndex(out, ChronicleTcp.SYNC_IDX_LEN, tailer.index());
                        tailer.finish();
                        break;

                    } else {
                        if (lastHeartbeat <= now) {
                            writeSizeAndIndex(out, ChronicleTcp.IN_SYNC_LEN, ChronicleTcp.IDX_NONE);
                            break;
                        }
                    }
                }
            } else {
                writeSizeAndIndex(out, ChronicleTcp.IN_SYNC_LEN, 0L);
            }
            return true;
        }

        return false;
    }

    protected boolean onMapping(Bytes in, SessionDetailsProvider sessionDetailsProvider) throws IOException {
        if (in.remaining() >= 4) {
            int size = in.readInt();
            if (in.remaining() >= size) {
                MappingProvider mappingProvider = sessionDetailsProvider.get(MappingProvider.class);
                if (mappingProvider != null) {
                    MappingFunction mappingFunction = in.readObject(MappingFunction.class);
                    mappingProvider.withMapping(mappingFunction);
                }

                return true;
            } else {
                in.position(in.position() - 4);
            }
        }
        return false;
    }

    /**
     * applies a mapping if the mapping is not set to {@code}null{code}
     *
     * @param source   the tailer for the mapping to be applied to
     * @param mappingProvider the key attachment
     * @return returns the tailer or the mapped bytes
     * @see
     */
    protected Bytes applyMapping(@NotNull final ExcerptTailer source,
                                 @Nullable MappingProvider mappingProvider) {
        if (mappingProvider == null) {
            return source;
        }

        final MappingFunction mappingFunction = mappingProvider.withMapping();
        if (mappingFunction == null) {
            return source;
        }

        withMappedBuffer.clear();
        if (withMappedBuffer.capacity() < source.limit()) {
            withMappedBuffer.resetToSize((int) source.capacity());
        }

        try {
            mappingFunction.apply(source, withMappedBuffer);
        } catch (IllegalArgumentException e) {
            // lets try to resize
            if (e.getMessage().contains("Attempt to write")) {
                if (withMappedBuffer.capacity() == Integer.MAX_VALUE) {
                    throw e;
                }

                withMappedBuffer.resetToSize(
                        Math.min(
                                Integer.MAX_VALUE,
                                (int) (withMappedBuffer.capacity() * 1.5))
                );

            } else {
                throw e;
            }
        }

        return withMappedBuffer.flip();
    }

    protected boolean hasRoomForExcerpt(Bytes bytes, Bytes tailer) {
        return hasRoomFor(bytes, tailer.remaining() + ChronicleTcp.HEADER_SIZE);
    }

    protected boolean hasRoomFor(Bytes bytes, long size) {
        return bytes.remaining() >= size;
    }

    public static VanillaSourceTcpHandler vanilla() {
        return new VanillaSourceTcpHandler();
    }

    public void setAppender(ExcerptAppender appender) {
        this.appender = appender;
    }

    public static class VanillaSourceTcpHandler extends SourceTcpHandler {

        private boolean nextIndex;

        @Override
        protected boolean write(Bytes out, SessionDetailsProvider sessionDetailsProvider) {
            if (nextIndex) {
                if (!tailer.nextIndex()) {
                    pauser.pause();
                    if (!tailer.nextIndex()) {
                        return false;
                    }
                }
            } else {
                if (!tailer.index(this.index)) {
                    return false;

                } else {
                    this.nextIndex = true;
                }
            }

            final MappingProvider mappingProvider = sessionDetailsProvider.get(MappingProvider.class);
            Bytes content = applyMapping(tailer, mappingProvider);
            int size = (int) content.limit();

            writeSizeAndIndex(out, size, tailer.index());

            // for large objects send one at a time.
            if (size > out.limit() / 2) {
                // TODO - this currently won't work
                while (size > 0) {
                    int minSize = (int) Math.min(size, out.remaining());
                    out.write(content);
                    // TODO
                    //                    connection.write(bytesOut.flip());
//                    bytesOut.clear();

                    size -= minSize;
                    if (size > 0) {
//                        bytesOut.clear();
                    }
                }
            } else {
                out.write(content);

                long previousIndex = tailer.index();
                long currentIndex;
                for (int count = maxExcerptsPerMessage; (count > 0) && tailer.nextIndex(); ) {
                    currentIndex = tailer.index();
                    content = applyMapping(tailer, mappingProvider);

                    // if there is free space, copy another one.
                    if (hasRoomForExcerpt(out, content)) {
                        size = (int) content.limit();
                        previousIndex = currentIndex;
                        writeSizeAndIndex(out, size, currentIndex);
                        out.write(content);
                        count--;

                        tailer.finish();
                    } else {
                        tailer.finish();
                        // if there is no space, go back to the previous index
                        tailer.index(previousIndex);
                        break;
                    }
                }

            }

/*
            if (bytesOut.remaining() > 0) {
                throw new EOFException("Failed to send index=" + tailer.index());
            }
*/

            return true;
        }

        @Override
        protected boolean onSubscribe(Bytes in, Bytes out, SessionDetailsProvider sessionDetailsProvider) {
            if (in.remaining() >= 8) {
                this.index = in.readLong();
                if (this.index == ChronicleTcp.IDX_TO_START) {
                    this.nextIndex = true;
                    this.tailer = tailer.toStart();
                    this.index = -1;

                } else if (this.index == ChronicleTcp.IDX_TO_END) {
                    this.nextIndex = false;
                    this.tailer = tailer.toEnd();
                    this.index = tailer.index();

                    if (this.index == -1) {
                        this.nextIndex = true;
                        this.tailer = tailer.toStart();
                        this.index = -1;
                    }
                } else {
                    this.nextIndex = false;
                }

                writeSizeAndIndex(out, ChronicleTcp.SYNC_IDX_LEN, this.index);

                subscriptionListener.subscribed();
                return true;
            } else {
                return false;
            }
        }

        @Override
        protected boolean onSubmit(Bytes in, Bytes out, boolean ack) throws IOException {
            if (in.remaining() >= 8) {
                long size = in.readLong();
                if (in.remaining() >= size) {
                    long inLimit = in.limit();
                    in.limit(in.position() + size);

                    try {
                        appender.startExcerpt((int) size);
                        appender.write(in);
                        appender.finish();
                    } finally {
                        in.limit(inLimit);
                    }

                    pauser.unpause();

                    if (ack) {
                        writeSizeAndIndex(out, ChronicleTcp.ACK_LEN, appender.lastWrittenIndex());
                    }

                    return true;
                } else {
                    in.position(in.position() - 8); // unread the size
                }
            }
            return false;
        }

    }

    public static IndexedSourceTcpHandler indexed() {
        return new IndexedSourceTcpHandler();
    }

    public static class IndexedSourceTcpHandler extends SourceTcpHandler {

        @Override
        protected boolean write(Bytes out, SessionDetailsProvider sessionDetailsProvider) {
            if (!tailer.index(index)) {
                if (tailer.wasPadding()) {
                    if (index >= 0) {
                        writeSizeAndIndex(out, ChronicleTcp.PADDED_LEN, tailer.index());
                    }

                    index++;
                }

                pauser.pause();

                if (!tailer.index(index)) {
                    return false;
                }
            }

            pauser.reset();

            final MappingProvider mappingProvider = sessionDetailsProvider.get(MappingProvider.class);
            Bytes content = applyMapping(tailer, mappingProvider);
            int size = (int) content.limit();

            writeSizeAndIndex(out, size, tailer.index());

            // for large objects send one at a time.
            if (size > out.capacity() / 2) {
                // TODO - this currently won't work
/*
                while (size > 0) {
                    int minSize = (int) Math.min(size, out.remaining());
                    out.write(bytes);

                    size -= minSize;
                    if (size > 0) {
                        bytesOut.clear();
                    }
                }
*/
            } else {
                out.write(content);
                for (int count = maxExcerptsPerMessage; (count > 0) && tailer.index(index + 1); ) {
                    if (!tailer.wasPadding()) {
                        content = applyMapping(tailer, mappingProvider);
                        // if there is free space, copy another one.
                        if (hasRoomForExcerpt(out, content)) {
                            size = (int) content.limit();
                            writeSizeAndIndex(out, size, tailer.index());
                            out.write(content);

                            index++;
                            count--;

                            tailer.finish();

                        } else {
                            break;
                        }
                    } else {
                        if (hasRoomFor(out, ChronicleTcp.HEADER_SIZE)) {
                            writeSizeAndIndex(out, ChronicleTcp.PADDED_LEN, index);
                        } else {
                            break;
                        }

                        index++;
                    }
                }

            }

            index++;
            return true;
        }

        @Override
        protected boolean onSubscribe(Bytes in, Bytes out, SessionDetailsProvider sessionDetailsProvider) {
            this.index = in.readLong();
            if (this.index == ChronicleTcp.IDX_TO_START) {
                this.index = -1;

            } else if (this.index == ChronicleTcp.IDX_TO_END) {
                this.index = tailer.toEnd().index();
            }

            writeSizeAndIndex(out, ChronicleTcp.SYNC_IDX_LEN, this.index);

            subscriptionListener.subscribed();
            return true;
        }

        @Override
        protected boolean onSubmit(Bytes in, Bytes out, boolean ack) throws IOException {
            if (ack) {
                writeSizeAndIndex(out, ChronicleTcp.NACK_LEN, ChronicleTcp.IDX_NOT_SUPPORTED);
            }

            return true;
        }
    }

    public interface SubscriptionListener {
        void subscribed();

        void unsubscribed();
    }

}
