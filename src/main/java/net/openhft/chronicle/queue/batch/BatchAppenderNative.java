package net.openhft.chronicle.queue.batch;

public class BatchAppenderNative implements BatchAppender {

    static {
        System.load("JniBatchAppender");
    }

    public native long writeMessages(long rawAddress, long rawMaxBytes, int rawMaxMessages);

}
