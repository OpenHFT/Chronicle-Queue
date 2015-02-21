/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package vanilla.java.tutorial;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.lang.model.Byteable;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static net.openhft.lang.model.DataValueClasses.newDirectInstance;
import static net.openhft.lang.model.DataValueClasses.newDirectReference;

public class DirectAccess {

    // *************************************************************************
    //
    // *************************************************************************

    public static void main(String[] ignored) throws Exception {
        final int items = 100;

        appendWithDirectInstance(items);
        appendWithDirectReference(items);
    }

    // *************************************************************************
    //
    // *************************************************************************

    private static void appendWithDirectInstance(final int items) throws Exception {
        final int readers = items / 10;

        final String path = System.getProperty("java.io.tmpdir") + "/direct-instance";
        final Event event = newDirectInstance(Event.class);

        try (Chronicle chronicle = ChronicleQueueBuilder.vanilla(path).build()) {
            chronicle.clear();

            ExcerptAppender appender = chronicle.createAppender();
            for(int i=0; i<items; i++) {
                event.bytes(appender, 0);
                event.setOwner(0);
                event.setType(i / 10);
                event.setTimestamp(System.currentTimeMillis());
                event.setId(i);

                appender.startExcerpt(event.maxSize());
                appender.write(event.bytes(), 0, event.maxSize());
                appender.finish();
            }

            appender.close();

            process(chronicle, items);
        }
    }

    private static void appendWithDirectReference(final int items) throws Exception {
        final String path = System.getProperty("java.io.tmpdir") + "/direct-instance";
        final Event event = newDirectReference(Event.class);

        try (Chronicle chronicle = ChronicleQueueBuilder.vanilla(path).build()) {
            chronicle.clear();

            ExcerptAppender appender = chronicle.createAppender();
            for(int i=0; i<items; i++) {
                appender.startExcerpt(event.maxSize());

                event.bytes(appender, 0);
                event.setOwner(0);
                event.setType(i / 10);
                event.setTimestamp(System.currentTimeMillis());
                event.setId(i);

                appender.position(event.maxSize());
                appender.finish();
            }

            appender.close();

            process(chronicle, items);
        }
    }

    private static void process(Chronicle chronicle, int items) throws Exception {
        final int readers = items / 10;
        ExecutorService ex = Executors.newFixedThreadPool(readers * 2);
        for (int i = 0; i < readers * 2; i++) {
            ex.execute(new Reader(chronicle, i, i / 2));
        }

        ex.shutdown();
        ex.awaitTermination(1, TimeUnit.MINUTES);

        try (ExcerptTailer tailer = chronicle.createTailer()) {
            final Event evt = newDirectReference(Event.class);
            for (int i = 0; i < items; ) {
                if (tailer.nextIndex()) {
                    evt.bytes(tailer, 0);
                    System.out.printf("read : %s\n", evt);
                    tailer.finish();
                    i++;
                }
            }
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    public static class Reader implements Runnable  {
        private final Chronicle chronicle;
        private final Random random;
        private final int id;
        private final int type;

        public Reader(final Chronicle chronicle, int id, int type) {
            this.chronicle = chronicle;
            this.random = new Random();
            this.id = id;
            this.type = type;
        }

        @Override
        public void run() {
            try (ExcerptTailer tailer = chronicle.createTailer()) {
                final Event event = newDirectReference(Event.class);
                while(tailer.nextIndex()) {
                    event.bytes(tailer, 0);
                    if(event.getType() == this.type) {
                        if (event.compareAndSwapOwner(0, this.id * 100)) {
                            event.compareAndSwapOwner(this.id * 100, this.id);
                            Thread.sleep(this.random.nextInt(250));
                        }
                    }

                    tailer.finish();
                }
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    public static interface Event extends Byteable {
        boolean compareAndSwapOwner(int expected, int value);
        int getOwner();
        void setOwner(int meta);

        void setId(long id);
        long getId();

        void setType(long id);
        long getType();

        void setTimestamp(long timestamp);
        long getTimestamp();
    }
}
