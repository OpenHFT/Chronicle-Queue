package net.openhft.chronicle.queue.bench;

public class BenchmarkUtils {

    /**
     * {@link Thread#join()} a thread and deal with interrupted exception
     *
     * @param t The thread to join
     */
    public static void join(Thread t) {
        try {
            t.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
