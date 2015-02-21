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

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static net.openhft.lang.model.DataValueClasses.newDirectInstance;
import static net.openhft.lang.model.DataValueClasses.newDirectReference;

public class DirectAccess {

    // *************************************************************************
    //
    // *************************************************************************

    public static void main(String[] ignored) {
        final int items = 1000;
        final String path = System.getProperty("java.io.tmpdir") + "/direct-access";
        //final StockPrice price  = newDirectReference(StockPrice.class);
        //appender.position(price.maxSize());

        final StockPrice price  = newDirectInstance(StockPrice.class);
        price.bytes().position(price.maxSize());

        try (Chronicle chronicle = ChronicleQueueBuilder.vanilla(path).build()) {
            chronicle.clear();

            ExcerptAppender appender = chronicle.createAppender();
            for(int i=0;i<items;i++) {
                price.bytes(appender, 0);
                price.setStockId(i / 10);
                price.setTransactionTime(System.currentTimeMillis());
                price.setPrice(i);
                price.setQuantity(i);

                appender.startExcerpt(price.maxSize());
                appender.write(price.bytes(),0, price.maxSize());
                appender.finish();
            }

            appender.close();

            ExecutorService ex = Executors.newFixedThreadPool(5);
            for(int i=0;i<(items / 100);i++) {
                ex.execute(new Reader(chronicle, i));
            }

            ex.awaitTermination(5, TimeUnit.MINUTES);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    public static class Reader implements Runnable  {
        private final Chronicle chronicle;
        private final int id;

        public Reader(final Chronicle chronicle, int id) {
            this.chronicle = chronicle;
            this.id = id;

        }

        @Override
        public void run() {
            System.out.println("Start reader id=" + this.id);
            try (ExcerptTailer tailer = chronicle.createTailer()) {
                final StockPrice price = newDirectReference(StockPrice.class);
                while(tailer.nextIndex()) {
                    price.bytes(tailer, 0);
                    if(price.getStockId() == this.id) {
                        if (price.compareAndSwapMeta(0, this.id)) {
                            System.out.printf("%d : %s - sotock-%d %f@%f\n",
                                this.id,
                                new Date(price.getTransactionTime()),
                                price.getStockId(),
                                price.getQuantity(),
                                price.getPrice()
                            );
                        }
                    }

                    tailer.finish();
                }
            } catch(IOException e) {
                e.printStackTrace();
            }
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    public static interface StockPrice extends Byteable {
        boolean compareAndSwapMeta(int expected, int value);
        int getMeta();
        void setMeta(int meta);

        void setStockId(long id);
        long getStockId();

        void setTransactionTime(long timestamp);
        long getTransactionTime();

        void setPrice(double price);
        double getPrice();

        void setQuantity(double quantity);
        double getQuantity();
    }
}
