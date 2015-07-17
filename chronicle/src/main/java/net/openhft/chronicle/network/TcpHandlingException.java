package net.openhft.chronicle.network;

import java.io.IOException;

/**
 * Created by lear on 15/07/2015.
 */
public class TcpHandlingException extends RuntimeException {

    public TcpHandlingException(String message) {
        super(message);
    }

    public TcpHandlingException(Throwable throwable) {
        super(throwable);
    }
}
