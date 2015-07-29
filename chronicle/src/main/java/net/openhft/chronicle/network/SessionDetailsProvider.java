package net.openhft.chronicle.network;

import java.net.InetSocketAddress;

/**
 * Taken from: https://github.com/OpenHFT/Chronicle-Network/blob/master/src/main/java/net/openhft/chronicle/network/api/session/SessionDetailsProvider.java
 *
 * for future integration ease
 */
public interface SessionDetailsProvider extends SessionDetails {

    void setConnectTimeMS(long connectTimeMS);

    void setClientAddress(InetSocketAddress connectionAddress);

    void setSecurityToken(String securityToken);

    void setUserId(String userId);
}
