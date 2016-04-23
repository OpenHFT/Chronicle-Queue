package net.openhft.chronicle.queue.service;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.wire.MethodReader;
import org.junit.Test;

import java.io.File;

import static org.easymock.EasyMock.*;

/**
 * Created by peter on 23/04/16.
 */
public class HelloWorldTest {
    @Test
    public void testViaMock() {
        HelloReplier replier = createMock(HelloReplier.class);
        replier.reply("Hello April");
        replier.reply("Hello June");
        replay(replier);

        HelloWorld helloWorld = new HelloWorldImpl(replier);
        helloWorld.hello("April");
        helloWorld.hello("June");
        verify(replier);
    }

    @Test
    public void testWithAsQueueService() {
        // acts as three processes in one test
        // process A writes to the HelloWorld interface.
        // process B read fromt he HelloWorld interface and writes to the
        String input = OS.TARGET + "/input-" + System.nanoTime();
        String output = OS.TARGET + "/output-" + System.nanoTime();

        HelloReplier replier = createMock(HelloReplier.class);
        replier.reply("Hello April");
        replier.reply("Hello June");
        replay(replier);

        ServiceWrapperBuilder<HelloReplier> builder = ServiceWrapperBuilder
                .serviceBuilder(input, output, HelloReplier.class, HelloWorldImpl::new)
                .inputSourceId(1).outputSourceId(2);

        try (CloseableHelloWorld helloWorld = builder.inputWriter(CloseableHelloWorld.class);
             MethodReader replyReader = builder.outputReader(replier);
             ServiceWrapper helloWorldService = builder.get()) {

            helloWorld.hello("April");
            helloWorld.hello("June");

            System.out.println(helloWorldService.inputQueues()[0].dump());
            for (int i = 0; i < 2; i++) {
                while (!replyReader.readOne()) {
                    Thread.yield();
                }
            }
            System.out.println(helloWorldService.outputQueue().dump());
            verify(replier);
        } finally {
            IOTools.deleteDirWithFiles(new File(input), 2);
            IOTools.deleteDirWithFiles(new File(output), 2);
        }
    }

    interface CloseableHelloWorld extends HelloWorld, Closeable {
    }
}
