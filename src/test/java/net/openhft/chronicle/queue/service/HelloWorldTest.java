/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.queue.service;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import org.junit.Test;

import java.io.File;

import static org.easymock.EasyMock.*;


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

//            System.out.println(helloWorldService.inputQueues()[0].dump());
            for (int i = 0; i < 2; i++) {
                while (!replyReader.readOne()) {
                    Thread.yield();
                }
            }
//            System.out.println(helloWorldService.outputQueue().dump());
            verify(replier);
        } finally {
            try {
                IOTools.deleteDirWithFiles(new File(input), 2);
                IOTools.deleteDirWithFiles(new File(output), 2);
            } catch (IORuntimeException e) {
                e.printStackTrace();
            }
        }
    }

    interface CloseableHelloWorld extends HelloWorld, Closeable {
    }
}
