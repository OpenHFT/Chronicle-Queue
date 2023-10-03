package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocumentHeaderData {

    private final Logger logger = LoggerFactory.getLogger(DocumentHeaderData.class);
    private long dynamicHeaderLengthPosition;
    private long dynamicHeaderLength;
    private long checksumOffset;

    public void onAppenderContextOpen(Bytes<?> bytes) {
        dynamicHeaderLengthPosition = bytes.writePosition();
        bytes.writeInt(0); // Reserve space for the dynamic header length
        appenderReserveDynamicHeader(bytes);
        dynamicHeaderLength = bytes.writePosition() - dynamicHeaderLengthPosition;
        bytes.writeInt(dynamicHeaderLengthPosition, (int) dynamicHeaderLength);
    }

    public void onAppenderContextClose(Bytes<?> bytes) {
        appenderCompleteHeader(bytes);
        reset();
    }

    public void onTailerContextOpen(Bytes<?> bytes) {
        int dynamicHeaderLength = bytes.readInt();
        tailerReadDynamicHeader(dynamicHeaderLength, bytes);
    }

    public void reset() {
        dynamicHeaderLengthPosition = -1;
        dynamicHeaderLength = -1;
        checksumOffset = -1;
    }

    public void appenderReserveDynamicHeader(Bytes<?> bytes) {
        checksumOffset = bytes.writePosition();
        bytes.writeLong(0);
    }

    public void appenderCompleteHeader(Bytes<?> bytes) {
        long checksum = computeChecksum(bytes);
        bytes.writeLong(checksumOffset, checksum);
    }

    public void tailerReadDynamicHeader(int dynamicHeaderLength, Bytes<?> bytes) {
        long checksum = bytes.readLong();
        if (logger.isTraceEnabled()) {
            logger.trace("Checksum: {}", checksum);
        }
    }

    private long computeChecksum(Bytes<?> bytes) {
        return 0;
    }

}
