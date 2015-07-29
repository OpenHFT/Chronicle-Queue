package net.openhft.chronicle.network;

import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.UUID;

/**
 * Taken from: https://github.com/OpenHFT/Chronicle-Network/blob/master/src/main/java/net/openhft/chronicle/network/api/session/SessionDetails.java
 *
 * for future integration ease.
 */
public interface SessionDetails {

    // a unique id used to identify this session, this field is by contract immutable
    UUID sessionId();

    @Nullable
    String userId();

    @Nullable
    String securityToken();

    @Nullable
    InetSocketAddress clientAddress();

    long connectTimeMS();

    <I> void set(Class<I> infoClass, I info);

    <I> I get(Class<I> infoClass);
}
