package net.openhft.chronicle.queue.channel;

public interface PubSub {
    void subscribe(Subscribe subscribe);

    void unsubscribe(String name);
}
