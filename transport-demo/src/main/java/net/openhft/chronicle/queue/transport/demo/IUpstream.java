package net.openhft.chronicle.queue.transport.demo;

/**
 * Interface for upstream messages for you core system
 */
public interface IUpstream {
    void onPrice(Price price);
}
