package net.openhft.chronicle.queue.impl.single;

/**
 * <p>
 * Strategy to handle the last acknowledged index replicated for a source queue.
 * </p>
 * Queue replication is a feature included in Chronicle Queue Enterprise. Please contact <a href="mailto:sales@chronicle.software">sales@chronicle.software</a> for more information.
 */
public interface LastAcknowledgedIndexReplicatedStrategy {

    /**
     * Called when an acknowledgment is received from a sink.
     * @param acknowledgeIndex The queue index that is being acknowledged
     * @param remoteIdentifier The identifier of the sink that is acknowledging the index
     * @return The last acknowledged index replicated or -1 to mark index as not acknowledged
     */
    long onAckReceived(long acknowledgeIndex, int remoteIdentifier);
}
