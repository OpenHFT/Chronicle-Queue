package net.openhft.chronicle.queue.impl.single;

public class WritePositionSequencePair {
    private final long writePosition;
    private final long sequenceNumber;

    public WritePositionSequencePair(long position, long sequenceNumber) {
        this.writePosition = position;
        this.sequenceNumber = sequenceNumber;
    }

    public long writePosition() {
        return writePosition;
    }

    public long sequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WritePositionSequencePair that = (WritePositionSequencePair) o;
        if (writePosition != that.writePosition) return false;
        return sequenceNumber == that.sequenceNumber;
    }
}
