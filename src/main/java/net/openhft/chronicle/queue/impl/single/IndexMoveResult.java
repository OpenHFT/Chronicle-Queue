package net.openhft.chronicle.queue.impl.single;

public class IndexMoveResult {
    private final long writePosition;
    private final long sequenceNumber;

    public IndexMoveResult(long position, long sequenceNumber) {
        this.writePosition = position;
        this.sequenceNumber = sequenceNumber;
    }

    public long writePosition() {
        return writePosition;
    }

    public long sequenceNumber() {
        return sequenceNumber;
    }
}
