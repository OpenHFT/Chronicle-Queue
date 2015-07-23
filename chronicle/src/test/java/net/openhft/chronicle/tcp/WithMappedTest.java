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
package net.openhft.chronicle.tcp;

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.*;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import net.openhft.lang.model.constraints.NotNull;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertFalse;

/**
 * @author Rob Austin.
 */
@RunWith(value = Parameterized.class)
public class WithMappedTest extends ChronicleTcpTestBase {

    public static final int TIMEOUT = 20;

    enum TypeOfQueue {INDEXED, VANILLA}

    private final TypeOfQueue typeOfQueue;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return asList(new Object[][]{ {TypeOfQueue.INDEXED}, {TypeOfQueue.VANILLA} });
    }

    public WithMappedTest(TypeOfQueue typeOfQueue) {
        this.typeOfQueue = typeOfQueue;
    }

    @Rule
    public final TestName testName = new TestName();

    final Collection<MarketData> marketRecords = loadMarketData();

    final Map<Date, MarketData> expectedMarketDate = expectedMarketData();

    private Map<Date, MarketData> expectedMarketData() {
        final Map<Date, MarketData> expectedMarketDate = new HashMap<Date, MarketData>();
        for (MarketData marketRecord : marketRecords) {
            expectedMarketDate.put(new Date(marketRecord.date), marketRecord);
        }

        return expectedMarketDate;
    }

    private static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    private static long _2014_01_01 = timeFrom("2014-01-01");
    private static long _2016_01_01 = timeFrom("2016-01-01");

    private static long timeFrom(String data) {
        try {
            return DATE_FORMAT.parse(data).getTime();
        } catch (ParseException e) {
            return -1;
        }
    }

    private static class HighLow implements BytesMarshallable {

        private long date;
        private double high;
        private double low;

        @Override
        public void readMarshallable(@NotNull Bytes in) throws IllegalStateException {
            date = in.readLong();
            high = in.readDouble();
            low = in.readDouble();
        }

        @Override
        public void writeMarshallable(@NotNull Bytes out) {
            out.writeLong(date);
            out.writeDouble(high);
            out.writeDouble(low);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            HighLow highLow = (HighLow) o;

            if (date != highLow.date) return false;

            return true;
        }

        private static MappingFunction fromMarketData() {
            return new MappingFunction() {
                @Override
                public void apply(Bytes from, Bytes to) {
                    //date
                    to.writeLong(from.readLong());

                    //open which we not send out
                    from.readDouble();

                    // high
                    to.writeDouble(from.readDouble());

                    //low
                    to.writeDouble(from.readDouble());
                }
            };
        }

        @Override
        public int hashCode() {
            return (int) (date ^ (date >>> 32));
        }

        @Override
        public String toString() {
            return "PriceData{" +
                    "date=" + new Date(date) +
                    ", high=" + high +
                    ", low=" + low +
                    '}';
        }
    }

    private static class Close implements BytesMarshallable {

        private long date;
        private double close;
        private double adjClose;

        @Override
        public void readMarshallable(@NotNull Bytes in) throws IllegalStateException {
            date = in.readLong();
            close = in.readDouble();
            adjClose = in.readDouble();
        }

        @Override
        public void writeMarshallable(@NotNull Bytes out) {
            out.writeLong(date);
            out.writeDouble(close);
            out.writeDouble(adjClose);
        }

        @Override
        public String toString() {
            return "PriceData{" +
                    "date=" + new Date(date) +
                    ", close=" + close +
                    ", adjClose=" + adjClose +
                    '}';
        }

        /**
         * applys a filter to only send the close and adjusted close
         *
         * @return filtered data
         */
        private static MappingFunction fromMarketData() {
            return new MappingFunction() {
                @Override
                public void apply(Bytes from, Bytes to) {
                    //date
                    to.writeLong(from.readLong());

                    //open - skipping
                    from.readDouble();

                    // high - skipping
                    from.readDouble();

                    // low - skipping
                    from.readDouble();

                    // close - skipping
                    to.writeDouble(from.readDouble());

                    // volume - skipping
                    from.readDouble();

                    // adjClose
                    to.writeDouble(from.readDouble());
                }
            };
        }

    }

    private static class MarketData implements BytesMarshallable {
        private long date;
        private double open;
        private double high;
        private double low;
        private double close;
        private double volume;
        private double adjClose;

        public MarketData() {
        }

        public MarketData(String... args) throws ParseException {
            int i = 0;
            try {
                date = DATE_FORMAT.parse(args[i++]).getTime();
                open = Double.valueOf(args[i++]);
                high = Double.valueOf(args[i++]);
                low = Double.valueOf(args[i++]);
                close = Double.valueOf(args[i++]);
                volume = Double.valueOf(args[i++]);
                adjClose = Double.valueOf(args[i]);
            } catch (Exception e) {
                throw e;
            }
        }

        @Override
        public void readMarshallable(@NotNull Bytes in) throws IllegalStateException {

            date = in.readLong();
            open = in.readDouble();
            high = in.readDouble();
            low = in.readDouble();
            close = in.readDouble();
            volume = in.readDouble();
            adjClose = in.readDouble();
        }

        @Override
        public void writeMarshallable(@NotNull Bytes out) {
            out.writeLong(date);
            out.writeDouble(open);
            out.writeDouble(high);
            out.writeDouble(low);
            out.writeDouble(close);
            out.writeDouble(volume);
            out.writeDouble(adjClose);

            // uncomment this if you want to see the bytes that are sent
            /*out.flip();
            System.out.println("writeMarshallable=>" + AbstractBytes.toHex(out));
            out.position(out.limit());
            out.limit(out.capacity()) ;*/

        }

        @Override
        public String toString() {
            return "MarketData{" +
                    "date=" + new Date(date) +
                    ", open=" + open +
                    ", high=" + high +
                    ", low=" + low +
                    ", close=" + close +
                    ", volume=" + volume +
                    ", adjClose=" + adjClose +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof MarketData)) return false;

            MarketData that = (MarketData) o;

            if (Double.compare(that.adjClose, adjClose) != 0) return false;
            if (Double.compare(that.close, close) != 0) return false;
            if (date != that.date) return false;
            if (Double.compare(that.high, high) != 0) return false;
            if (Double.compare(that.low, low) != 0) return false;
            if (Double.compare(that.open, open) != 0) return false;
            if (Double.compare(that.volume, volume) != 0) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            result = (int) (date ^ (date >>> 32));
            temp = Double.doubleToLongBits(open);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            temp = Double.doubleToLongBits(high);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            temp = Double.doubleToLongBits(low);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            temp = Double.doubleToLongBits(close);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            temp = Double.doubleToLongBits(volume);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            temp = Double.doubleToLongBits(adjClose);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            return result;
        }
    }

    Collection<MarketData> loadMarketData() {
        InputStream resourceAsStream = this.getClass().getClassLoader().getResourceAsStream("ftse-prices.csv");

        BufferedReader br = new BufferedReader(new InputStreamReader(resourceAsStream));

        ArrayList<MarketData> result = new ArrayList<MarketData>();
        String line;
        try {
            while ((line = br.readLine()) != null) {
                String[] split = line.split(",");

                // skip records that start with a #
                if (split[0].trim().startsWith("#"))
                    continue;

                result.add(new MarketData(split));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    @Test
    public void testReplicationWithPriceMarketDataFilter() throws Throwable {
        final String sourceBasePath;
        final String sinkHighLowBasePath;
        final String sinkCloseBasePath;

        final String testId = "testReplicationWithPriceMarketDataFilter";
        if(typeOfQueue == TypeOfQueue.VANILLA) {
            sourceBasePath = getVanillaTestPath(testId, "source");
            sinkHighLowBasePath = getVanillaTestPath(testId, "sink-highlow");
            sinkCloseBasePath = getVanillaTestPath(testId, "sink-close");

        } else {
            sourceBasePath = getIndexedTestPath(testId, "source");
            sinkHighLowBasePath = getIndexedTestPath(testId, "sink-highlow");
            sinkCloseBasePath = getIndexedTestPath(testId, "sink-close");
        }

        final ChronicleTcpTestBase.PortSupplier portSupplier = new ChronicleTcpTestBase.PortSupplier();

        final Chronicle source = source(sourceBasePath)
                .bindAddress(0)
                .connectionListener(portSupplier)
                .build();

        final int port = portSupplier.getAndAssertOnError();

        try {
            Callable<Void> appenderCallable = new Callable<Void>() {
                public Void call()   {
                    AffinityLock lock = AffinityLock.acquireLock();
                    try {
                        final ExcerptAppender appender = source.createAppender();
                        for (MarketData marketData : marketRecords) {
                            appender.startExcerpt();
                            marketData.writeMarshallable(appender);
                            appender.finish();
                        }

                        appender.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        lock.release();
                    }
                    return null;
                }
            };

            Callable<Void> highLowCallable = new Callable<Void>() {
                public Void call()   {
                    try {
                        final Chronicle highLowSink = sink(sinkHighLowBasePath)
                                .withMapping(HighLow.fromMarketData()) // this is sent to the source
                                .connectAddress("localhost", port)
                                .build();

                        AffinityLock lock = AffinityLock.acquireLock();
                        try (final ExcerptTailer tailer = highLowSink.createTailer()) {
                            while (tailer.nextIndex()) {
                                HighLow actual = new HighLow();
                                actual.readMarshallable(tailer);

                                // check the data is reasonable
                                Assert.assertTrue(actual.date > DATE_FORMAT.parse("2014-01-01").getTime());
                                Assert.assertTrue(actual.date < DATE_FORMAT.parse("2016-01-01").getTime());

                                Assert.assertTrue(actual.high > 5000);
                                Assert.assertTrue(actual.high < 8000);

                                Assert.assertTrue(actual.low > 5000);
                                Assert.assertTrue(actual.low < 8000);

                                MarketData expected = expectedMarketDate.get(new Date(actual.date));
                                Assert.assertEquals(expected.high, actual.high, 0.0);
                                Assert.assertEquals(expected.low, actual.low, 0.0);

                                tailer.finish();
                            }
                        } finally {
                            lock.release();
                            highLowSink.close();
                            highLowSink.clear();
                        }
                    } catch(Exception e) {
                        LOGGER.warn("", e);
                    }

                    return null;
                }
            };

            Callable<Void> closeCallable = new Callable<Void>() {
                public Void call()   {
                    try {
                        final Chronicle closeSink = sink(sinkCloseBasePath)
                                .withMapping(Close.fromMarketData()) // this is sent to the source
                                .connectAddress("localhost", port)
                                .build();

                        AffinityLock lock = AffinityLock.acquireLock();
                        try(final ExcerptTailer tailer = closeSink.createTailer()) {
                            while (tailer.nextIndex()) {
                                Close actual = new Close();
                                actual.readMarshallable(tailer);

                                // check the data is reasonable

                                Assert.assertTrue(actual.date > _2014_01_01);
                                Assert.assertTrue(actual.date < _2016_01_01);

                                Assert.assertTrue(actual.adjClose > 5000);
                                Assert.assertTrue(actual.adjClose < 8000);

                                Assert.assertTrue(actual.close > 5000);
                                Assert.assertTrue(actual.close < 8000);

                                final MarketData expected = expectedMarketDate.get(new Date(actual.date));

                                String message = "expected=" + expected + "actual=" + actual;

                                Assert.assertEquals(message, expected.adjClose, actual.adjClose, 0.0);
                                Assert.assertEquals(message, expected.close, actual.close, 0.0);

                                tailer.finish();
                            }
                        }finally {
                                lock.release();
                            closeSink.close();
                                closeSink.clear();
                            }
                    } catch(Exception e) {
                        LOGGER.warn("", e);
                    }

                    return null;
                }

            };

            try {

                ThreadFactory appenderFactory = new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "appender");
                    }
                };

                Future<Void> appenderFuture = Executors.newSingleThreadExecutor(appenderFactory).submit(appenderCallable);
                appenderFuture.get(20, TimeUnit.SECONDS);

                ThreadFactory closeFactory = new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "close");
                    }
                };

                Future<Void> closeFuture = Executors.newSingleThreadExecutor(closeFactory).submit(closeCallable);

                ThreadFactory highLowFactory = new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "highlow");
                    }
                };

                Future<Void> highLowFuture = Executors.newSingleThreadExecutor(highLowFactory).submit(highLowCallable);

                closeFuture.get(20, TimeUnit.SECONDS);
                highLowFuture.get(20, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                throw e.getCause();
            }

        } finally {
            source.close();
            source.clear();

            if(typeOfQueue == TypeOfQueue.VANILLA) {
                assertFalse(new File(sourceBasePath).exists());
                assertFalse(new File(sinkCloseBasePath).exists());
                assertFalse(new File(sinkHighLowBasePath).exists());

            } else {
                assertIndexedClean(sourceBasePath);
                assertIndexedClean(sinkCloseBasePath);
                assertIndexedClean(sinkHighLowBasePath);
            }
        }
    }

    /**
     * @return a filters records to only send records that contain an even data ( for example will
     * send records that contain a date with 2nd and 4th in the month not a 3rd ) this is just to
     * show we we can use filters to skip records as well.
     */
    private static MappingFunction evenDayFilter() {
        return new MappingFunction() {
            @Override
            public void apply(Bytes from, Bytes to) {
                //date
                long v = from.readLong();

                Date date = new Date(v);
                int day = date.getDay();

                boolean isEvenDay = (day % 2) == 0;

                if (!isEvenDay)
                    return;

                to.writeLong(v);

                //open - skipping
                to.writeDouble(from.readDouble());

                // high - skipping
                to.writeDouble(from.readDouble());

                // low - skipping
                to.writeDouble(from.readDouble());

                // close - skipping
                to.writeDouble(from.readDouble());

                // volume - skipping
                to.writeDouble(from.readDouble());

                // adjClose
                to.writeDouble(from.readDouble());
            }
        };
    }

    @Test
    public void testReplicationWithEvenDayFilter() throws Throwable {
        final String sourceBasePath;
        final String sinkHighLowBasePath;

        final String testId = "testReplicationWithEvenDayFilter";
        if(typeOfQueue == TypeOfQueue.VANILLA) {
            sourceBasePath = getVanillaTestPath(testId, "source");
            sinkHighLowBasePath = getVanillaTestPath(testId, "sink-highlow");

        } else {
            sourceBasePath = getIndexedTestPath(testId, "source");
            sinkHighLowBasePath = getIndexedTestPath(testId, "sink-highlow");
        }

        final ChronicleTcpTestBase.PortSupplier portSupplier = new ChronicleTcpTestBase.PortSupplier();
        final Chronicle source = source(sourceBasePath)
                .bindAddress(0)
                .connectionListener(portSupplier)
                .build();

        final int port = portSupplier.getAndAssertOnError();
        try {
            final Collection<MarketData> marketRecords = loadMarketData();
            final Map<Date, MarketData> expectedMarketDate = new HashMap<>();
            for (MarketData marketRecord : marketRecords) {
                expectedMarketDate.put(new Date(marketRecord.date), marketRecord);
            }

            Callable<Void> appenderCallable = new Callable<Void>() {
                public Void call()   {
                    AffinityLock lock = AffinityLock.acquireLock();
                    try {
                        final ExcerptAppender appender = source.createAppender();
                        for (MarketData marketData : marketRecords) {
                            appender.startExcerpt();
                            marketData.writeMarshallable(appender);
                            appender.finish();
                        }

                        appender.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        lock.release();
                    }
                    return null;
                }
            };

            Callable<Void> dayFilterCallable = new Callable<Void>() {
                public Void call()   {

                    try {
                        final Chronicle highLowSink = sink(sinkHighLowBasePath)
                                .withMapping(evenDayFilter()) // this is sent to the source
                                .connectAddress("localhost", port)
                                .build();

                        AffinityLock lock = AffinityLock.acquireLock();

                        try (final ExcerptTailer tailer = highLowSink.createTailer()) {
                            while (tailer.nextIndex()) {
                                // skip the empty messages
                                if (tailer.limit() == 0) {
                                    continue;
                                }

                                MarketData actual = new MarketData();
                                actual.readMarshallable(tailer);

                                //check that he date is even
                                Assert.assertTrue(actual.date % 2 == 0);

                                // check the data is reasonable
                                Assert.assertTrue(actual.date > DATE_FORMAT.parse("2014-01-01").getTime());

                                final MarketData expected = expectedMarketDate.get(new Date(actual.date));
                                Assert.assertEquals(expected, actual);

                                tailer.finish();
                            }
                        } finally {
                            lock.release();
                            highLowSink.close();
                            highLowSink.clear();
                        }
                    } catch(Exception e) {
                        LOGGER.warn("", e);
                    }
                    return null;
                }
            };

            try {
                ThreadFactory appenderFactory = new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "appender");
                    }
                };

                Future<Void> appenderFuture = Executors.newSingleThreadExecutor(appenderFactory).submit(appenderCallable);
                appenderFuture.get(20, TimeUnit.SECONDS);

                ThreadFactory dayFilterFactory = new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "dayFilter");
                    }
                };

                Future<Void> dayFilterFuture = Executors.newSingleThreadExecutor(dayFilterFactory).submit(dayFilterCallable);
                dayFilterFuture.get(TIMEOUT, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                throw e.getCause();
            }

        } finally {
            source.close();
            source.clear();

            // check cleanup
            if(typeOfQueue == TypeOfQueue.VANILLA) {
                assertFalse(new File(sourceBasePath).exists());
                assertFalse(new File(sinkHighLowBasePath).exists());

            } else {
                assertIndexedClean(sourceBasePath);
                assertIndexedClean(sinkHighLowBasePath);
            }
        }
    }

    private ChronicleQueueBuilder.ReplicaChronicleQueueBuilder sink(@NotNull String path) {
        switch (typeOfQueue) {
            case VANILLA:
                return ChronicleQueueBuilder.vanilla(path).sink();
            case INDEXED:
                return ChronicleQueueBuilder.indexed(path).sink();
            default:
                throw new UnsupportedOperationException();
        }
    }

    private ChronicleQueueBuilder.ReplicaChronicleQueueBuilder source(@NotNull String path) {
        switch (typeOfQueue) {
            case VANILLA:
                return ChronicleQueueBuilder.vanilla(path).source();
            case INDEXED:
                return ChronicleQueueBuilder.indexed(path).source();
            default:
                throw new UnsupportedOperationException();
        }
    }
}

