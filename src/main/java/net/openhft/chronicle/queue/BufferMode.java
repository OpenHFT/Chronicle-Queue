package net.openhft.chronicle.queue;

public enum BufferMode {
    None,    // The default

    Copy,    //  used in conjunction with encryption

    Asynchronous   // used by chronicle-ring [ which is an enterprise product ]
}
