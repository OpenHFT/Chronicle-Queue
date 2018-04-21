package net.openhft.load;

import com.sun.jna.Native;
import com.sun.jna.Platform;

import java.io.IOException;

public final class MlockAll {
    private static final int MS_SYNC = 4;

    static {
        Native.register(MlockAll.class, Platform.C_LIBRARY_NAME);
    }

    private MlockAll() {
    }

    public static void doMlockall() throws IOException {
        if (mlockall(1) != 0)
            throw new IOException("mlockall failed: error code " + Native.getLastError());
    }

    private static native int mlockall(int flags);
}