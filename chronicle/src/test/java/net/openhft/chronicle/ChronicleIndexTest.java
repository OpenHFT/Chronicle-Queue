package net.openhft.chronicle;

import net.openhft.chronicle.Excerpt;
import net.openhft.chronicle.VanillaChronicle;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class ChronicleIndexTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws IOException {
        VanillaChronicle chronicle = new VanillaChronicle(folder.newFolder().getAbsolutePath());
        VanillaChronicle.VanillaAppender appender = chronicle.createAppender();
        appender.startExcerpt();
        appender.writeUTF("This is a test");
        appender.finish();

        long lastIndex = appender.lastWrittenIndex();
        Assert.assertTrue("lastIndex: "+ lastIndex, lastIndex >= 0);
        Assert.assertEquals(lastIndex+1, appender.index());

        Excerpt excerpt = chronicle.createExcerpt();
        Assert.assertTrue(excerpt.index(lastIndex));
        String utf = excerpt.readUTF();

        assertThat(utf, equalTo("This is a test"));
        folder.delete();
    }
}