package net.openhft.chronicle.queue.channel;

import net.openhft.chronicle.wire.SelfDescribingMarshallable;

public class Subscribe extends SelfDescribingMarshallable {
    private String eventType;
    private String name;
    private boolean priority;

    public String eventType() {
        return eventType;
    }

    public Subscribe eventType(String eventType) {
        this.eventType = eventType;
        return this;
    }

    public String name() {
        return name;
    }

    public Subscribe name(String name) {
        this.name = name;
        return this;
    }

    public boolean priority() {
        return priority;
    }

    public Subscribe priority(boolean priority) {
        this.priority = priority;
        return this;
    }
}
