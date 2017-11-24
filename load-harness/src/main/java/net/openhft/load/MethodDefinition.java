package net.openhft.load;

import net.openhft.load.messages.EightyByteMessage;

public interface MethodDefinition {
    void onEightyByteMessage(final EightyByteMessage message);
}
