package net.openhft.chronicle.network;

import java.net.InetSocketAddress;

/**
 * Created by lear on 10/07/15.
 */
public interface SessionDetailsProvider extends SessionDetails {

    void setConnectTimeMS(long connectTimeMS);

    void setClientAddress(InetSocketAddress connectionAddress);

    void setSecurityToken(String securityToken);

    void setUserId(String userId);
}
