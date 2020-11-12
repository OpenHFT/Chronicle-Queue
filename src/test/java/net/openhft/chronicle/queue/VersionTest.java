package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.pom.PomPropertiesUtil;
import org.junit.Test;

import static org.junit.Assert.assertNotEquals;

public class VersionTest {

    @Test
    public void version() {
        final String version = PomPropertiesUtil.version("queue");
        assertNotEquals("unknown", version);
    }

}
