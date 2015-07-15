package net.openhft.chronicle.network;

import net.openhft.lang.io.Bytes;

/**
 * Created by lear on 10/07/15.
 */
public interface PipelineContext {

    void next(Bytes in, Bytes out, SessionDetailsProvider sessionDetailsProvider);

    void cancel();

    void done();

}
