package net.openhft.chronicle.queue.impl.single;

public class NamedTailerNotAvailableException extends IllegalStateException {

    private final String tailerName;

    private final Reason reason;

    public NamedTailerNotAvailableException(String tailerName, Reason reason) {
        super("Named tailer cannot be created because: " + reason.description);
        this.tailerName = tailerName;
        this.reason = reason;
    }

    public String tailerName() {
        return tailerName;
    }

    public Reason reason() {
        return reason;
    }

    public enum Reason {

        NOT_AVAILABLE_ON_SINK("Named tailers cannot be instantiated on a replication sink");

        private final String description;

        Reason(String description) {
            this.description = description;
        }
    }
}
