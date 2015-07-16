package net.openhft.chronicle.tcp;

import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.network.SessionDetailsProvider;
import net.openhft.chronicle.network.TcpHandler;
import net.openhft.chronicle.network.TcpHandlingException;
import net.openhft.lang.io.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by lear on 15/07/2015.
 */
public class SinkTcpRemoteAppenderHandler implements TcpHandler {

    private static final Logger logger = LoggerFactory.getLogger(SinkTcpRemoteAppenderHandler.class);

    private ExcerptAppender appender;

    private boolean appendRequireAck;

    private boolean waitingForAck;

    private long lastIndex;

    public SinkTcpRemoteAppenderHandler(ExcerptAppender appender) {
        this.appender = appender;
    }

    @Override
    public void process(Bytes in, Bytes out, SessionDetailsProvider sessionDetailsProvider) {
        processIncoming(in);
        processOutgoing(out);
    }

    private void processIncoming(Bytes in) {
        if (waitingForAck && in.remaining() >= ChronicleTcp.HEADER_SIZE) {

            int recType = in.readInt();
            long recIndex = in.readLong();

            switch (recType) {
                case ChronicleTcp.ACK_LEN:
                    this.lastIndex = recIndex;
                    waitingForAck = false;
                    break;

                case ChronicleTcp.NACK_LEN:
                    waitingForAck = false;
                    throw new IllegalStateException(
                            "Message discarded by server, reason: " + (
                                    recIndex == ChronicleTcp.IDX_NOT_SUPPORTED
                                            ? "unsupported"
                                            : "unknown")
                    );
                default:
                    logger.warn("Unknown message received {}, {}", recType, recIndex);
            }
        }
    }

    private void processOutgoing(Bytes out) {
        if (appender.remaining() > 0) {
            // only write anything if there is anything to write
            out.writeLong(appendRequireAck ? ChronicleTcp.ACTION_SUBMIT : ChronicleTcp.ACTION_SUBMIT_NOACK);
            out.writeLong(appender.limit());
            out.write(appender);

            if (appender.remaining() > 0) {
                throw new TcpHandlingException("Failed to write content for index " + appender.index());
            }

            waitingForAck = appendRequireAck;
        }
    }

    @Override
    public void onEndOfConnection() {

    }

    public void setAppendRequireAck(boolean appendRequireAck) {
        this.appendRequireAck = appendRequireAck;
    }

    public boolean waitingForAck() {
        return waitingForAck;
    }

    public long getLastIndex() {
        return lastIndex;
    }
}
