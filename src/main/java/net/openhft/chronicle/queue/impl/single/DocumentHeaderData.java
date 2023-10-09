package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocumentHeaderData {

    private static final int DATA_VERSION_DYNAMIC_HEADERS_INTRODUCED = 2;
    private final Logger logger = LoggerFactory.getLogger(DocumentHeaderData.class);
    private long dynamicHeaderLengthPosition;
    private long dynamicHeaderLength;
    private long checksumOffset;

    public DocumentHeaderData() {

    }

    private boolean dynamicHeadersSupported(SingleChronicleQueueStore store) {
        return store.dataVersion() >= DATA_VERSION_DYNAMIC_HEADERS_INTRODUCED;
    }

    public void onAppenderContextOpen(Bytes<?> bytes, SingleChronicleQueueStore store) {
        if (dynamicHeadersSupported(store)) {
            dynamicHeaderLengthPosition = bytes.writePosition();
            bytes.writeInt(0); // Reserve space for the dynamic header length
            appenderReserveDynamicHeader(bytes);
            dynamicHeaderLength = bytes.writePosition() - dynamicHeaderLengthPosition;
            bytes.writeInt(dynamicHeaderLengthPosition, (int) dynamicHeaderLength);
        }
    }

    public void onAppenderContextClose(Bytes<?> bytes, SingleChronicleQueueStore store) {
        if (dynamicHeadersSupported(store)) {
            appenderCompleteHeader(bytes);
            reset();
        }
    }

    public void onTailerContextOpen(Bytes<?> bytes, SingleChronicleQueueStore store) {
        if (dynamicHeadersSupported(store)) {
            int dynamicHeaderLength = bytes.readInt();
            tailerReadDynamicHeader(dynamicHeaderLength, bytes);
        }
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
