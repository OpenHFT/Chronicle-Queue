package net.openhft.chronicle.queue.micros;

/**
 * Created by peter on 24/03/16.
 */
public interface OrderListener {
    void onOrder(Order order);
}
