package net.openhft.chronicle.network;

import net.openhft.lang.io.Bytes;

/**
 * Based on https://github.com/OpenHFT/Chronicle-Network/blob/master/src/main/java/net/openhft/chronicle/network/api/TcpHandler.java
 *
 * for ease of eventual alignment
 */
public interface TcpHandler {

    void process(Bytes in, Bytes out, SessionDetailsProvider sessionDetailsProvider);

    // Still only Java 7 support, can't use default methods
    void onEndOfConnection();

}
