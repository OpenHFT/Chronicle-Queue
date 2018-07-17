package net.openhft.chronicle.queue.batch;

public class BatchAppenderNative implements BatchAppender {

    static {

        System.load(BatchAppenderNative.class.getResource(".").getFile() +
                "../../../../../../../c++/lib/libBatchAppenderNative.dylib");
    }

    public native long writeMessages(long rawAddress, long rawMaxBytes, int rawMaxMessages);

}
