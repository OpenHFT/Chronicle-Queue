package net.openhft.chronicle.queue.impl.single;

public interface LastAcknowledgedIndexReplicatedStrategy {
    long onAckReceived(long acknowledgeIndex, int remoteIdentifier);
}
