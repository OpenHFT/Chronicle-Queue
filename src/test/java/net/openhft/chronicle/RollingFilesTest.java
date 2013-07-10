package net.openhft.chronicle;

import net.openhft.lang.Jvm;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * @author peter.lawrey
 */
public class RollingFilesTest {
    @Test
    @Ignore
    public void findLast() throws IOException {
        String dirPath = Jvm.TMP + File.separatorChar + "findLast";
        ChronicleTools.deleteDirOnExit(dirPath);
        RollingChronicle chronicle = new RollingChronicle(dirPath, ChronicleConfig.TEST);
        chronicle.close();

    }

    @Test
    @Ignore
    public void rollOnDemand() throws IOException {
        String dirPath = Jvm.TMP + File.separatorChar + "rollOnDemand";
        ChronicleTools.deleteDirOnExit(dirPath);
        RollingChronicle chronicle = new RollingChronicle(dirPath, ChronicleConfig.TEST);

    }
}
