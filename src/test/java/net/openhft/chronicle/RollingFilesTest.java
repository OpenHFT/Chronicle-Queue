package net.openhft.chronicle;

import net.openhft.lang.Jvm;
import org.junit.Test;

import java.io.IOException;

/**
 * @author peter.lawrey
 */
public class RollingFilesTest {
    @Test
    public void rollOnDemand() throws IOException {
        RollingChronicle chronicle = new RollingChronicle(Jvm.TMP + "/rollOnDemand", ChronicleConfig.TEST);

    }
}
