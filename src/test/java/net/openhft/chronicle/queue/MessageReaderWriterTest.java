package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.AbstractMarshallable;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by Peter on 25/03/2016.
 */
public class MessageReaderWriterTest {

    @Test
    public void testWriteWhileReading() {
        ClassAliasPool.CLASS_ALIASES.addAlias(Message1.class, "M1");
        ClassAliasPool.CLASS_ALIASES.addAlias(Message2.class, "M2");

        String path = OS.TARGET + "/testWriteWhileReading-" + System.nanoTime() + "-";

        try (SingleChronicleQueue queue1 = SingleChronicleQueueBuilder.binary(path + "1").build();
             SingleChronicleQueue queue2 = SingleChronicleQueueBuilder.binary(path + "2").build()) {
            MethodReader reader2 = queue1.createTailer().methodReader(ObjectUtils.printAll(MessageListener.class));
            MessageListener writer2 = queue2.createAppender().methodWriter(MessageListener.class);
            MessageListener processor = new MessageProcessor(writer2);
            MethodReader reader1 = queue1.createTailer().methodReader(processor);
            MessageListener writer1 = queue1.createAppender().methodWriter(MessageListener.class);

            for (int i = 0; i < 3; i++) {
                // write a message
                writer1.method1(new Message1("hello"));
                writer1.method2(new Message2(234));

                // read those messages
                assertTrue(reader1.readOne());
                assertTrue(reader1.readOne());
//                System.out.println(queue1.dump());
                assertFalse(reader1.readOne());

                // read the produced messages
                assertTrue(reader2.readOne());
                assertTrue(reader2.readOne());
                assertFalse(reader2.readOne());
            }
        } finally {
            IOTools.shallowDeleteDirWithFiles(path + "1");
            IOTools.shallowDeleteDirWithFiles(path + "2");
        }
    }

    interface MessageListener {
        void method1(Message1 message);

        void method2(Message2 message);
    }

    static class Message1 extends AbstractMarshallable {
        String text;

        public Message1(String text) {
            this.text = text;
        }
    }

    static class Message2 extends AbstractMarshallable {
        long number;

        public Message2(long number) {
            this.number = number;
        }
    }

    static class MessageProcessor implements MessageListener {
        private final MessageListener writer2;

        public MessageProcessor(MessageListener writer2) {
            this.writer2 = writer2;
        }

        @Override
        public void method1(Message1 message) {
            message.text += "-processed";
            writer2.method1(message);
        }

        @Override
        public void method2(Message2 message) {
            message.number += 1000;
            writer2.method2(message);
        }
    }
}
