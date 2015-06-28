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
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.lang.model.Byteable;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static net.openhft.lang.model.DataValueClasses.newDirectReference;

public class OffHeapHelper {

    protected static void process(Chronicle chronicle, int items)   {
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

    protected static class Reader implements Runnable  {
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
