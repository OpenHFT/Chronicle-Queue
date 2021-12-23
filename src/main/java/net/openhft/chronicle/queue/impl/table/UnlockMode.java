package net.openhft.chronicle.queue.impl.table;

/**
 * Determines the action to take if lock acquisition times out
 */
public enum UnlockMode {
    /**
     * throw exception
     */
    ALWAYS,
    /**
     * force unlock and re-acquire
     */
    NEVER,
    /**
     * force unlock and re-acquire only if the locking process is dead, otherwise throw exception
     */
    LOCKING_PROCESS_DEAD
}
