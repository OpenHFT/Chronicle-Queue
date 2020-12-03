package net.openhft.chronicle.queue.batch;

import net.openhft.chronicle.core.OS;

@Deprecated() // "to be removed in version 21.x"
public class BatchAppenderNative implements BatchAppender {

    static {
        if (OS.isMacOSX())
            System.load(BatchAppenderNative.class.getResource(".").getFile() +
                    "../../../../../../../c++/lib/libBatchAppenderNative.dylib");
    }

    public native long writeMessages(long rawAddress, long rawMaxBytes, int rawMaxMessages);

}
