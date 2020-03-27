package net.openhft.chronicle.queue;

/*
 * Created by Peter Lawrey on 17/01/2017.
 */
public enum BufferMode {
    None,    // The default

    Copy,    //  used in conjunction with encryption 

    Asynchronous   // used by chronicle-ring [ which is an enterprise product ]
}
