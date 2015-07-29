package net.openhft.chronicle.network;

import net.openhft.lang.io.Bytes;

/**
 * Context to be passed through the pipeline of tcp handlers
 *
 * Author:  Ryan Lea
 */
public interface PipelineContext {

    void next(Bytes in, Bytes out, SessionDetailsProvider sessionDetailsProvider);

    void cancel();

    void done();

}
