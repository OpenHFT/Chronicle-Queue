package net.openhft.chronicle.network;

/**
 * Author: Ryan Lea
 */
public class TcpHandlingException extends RuntimeException {

    public TcpHandlingException(String message) {
        super(message);
    }

    public TcpHandlingException(Throwable throwable) {
        super(throwable);
    }
}
