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

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.assertFalse;

/**
 * @author Rob Austin.
 */
public class WithMappedTest extends ChronicleTcpTestBase {

    @Rule
    public final TestName testName = new TestName();

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


        public MarketData(String... args) throws ParseException {
            int i = 0;
            date = DATE_FORMAT.parse(args[i++]).getTime();
            open = Double.valueOf(args[i++]);
            high = Double.valueOf(args[i++]);
            low = Double.valueOf(args[i++]);
            close = Double.valueOf(args[i++]);
            volume = Double.valueOf(args[i++]);
            adjClose = Double.valueOf(args[i]);
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
    }


    private Collection<MarketData> loadMarketData() throws IOException, ParseException {
        InputStream resourceAsStream = this.getClass().getClassLoader().getResourceAsStream("ftse-prices.csv");

        BufferedReader br = new BufferedReader(new InputStreamReader(resourceAsStream));

        ArrayList<MarketData> result = new ArrayList<MarketData>();
        String line;
        while ((line = br.readLine()) != null) {
            String[] split = line.split(",");

            // skip records that start with a #
            if (split[0].trim().startsWith("#"))
                continue;

            result.add(new MarketData(split));
        }
        return result;

    }


    @Test
    public void testReplicationWithPriceMarketDataFilter() throws Throwable {

        final String sourceBasePath = getVanillaTestPath("-source");
        final String sinkHighLowBasePath = getVanillaTestPath("-sink-highlow");
        final String sinkCloseBasePath = getVanillaTestPath("-sink-close");

        final ChronicleTcpTestBase.PortSupplier portSupplier = new ChronicleTcpTestBase.PortSupplier();

        final Chronicle source = ChronicleQueueBuilder.vanilla(sourceBasePath)
                .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
                .build();

        final int port = portSupplier.getAndCheckPort();


        try {


            final Collection<MarketData> marketRecords = loadMarketData();

            final Map<Date, MarketData> expectedMarketDate = new HashMap<Date, MarketData>();

            for (MarketData marketRecord : marketRecords) {
                expectedMarketDate.put(new Date(marketRecord.date), marketRecord);
            }


            Callable<Void> appenderCallable = new Callable<Void>() {
                public Void call() throws Exception {


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

                public Void call() throws Exception {

                    final Chronicle highLowSink = ChronicleQueueBuilder.vanilla(sinkHighLowBasePath)
                            .sink()
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
                        highLowSink.clear();
                    }
                    return null;
                }


            };

            Callable<Void> closeCallable = new Callable<Void>() {

                public Void call() throws Exception {

                    final Chronicle closeSink = ChronicleQueueBuilder.vanilla(sinkCloseBasePath)
                            .sink()
                            .withMapping(Close.fromMarketData()) // this is sent to the source
                            .connectAddress("localhost", port)
                            .build();

                    AffinityLock lock = AffinityLock.acquireLock();
                    try (final ExcerptTailer tailer = closeSink.createTailer()) {

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


                            final MarketData expected = expectedMarketDate.get(new Date(actual
                                    .date));

                            String message = "expected=" + expected + "actual=" + actual;

                            Assert.assertEquals(message, expected.adjClose, actual.adjClose, 0.0);
                            Assert.assertEquals(message, expected.close, actual.close, 0.0);

                            tailer.finish();

                        }

                    } finally {
                        lock.release();
                        closeSink.clear();
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

            // check cleanup
            assertFalse(new File(sourceBasePath).exists());
            assertFalse(new File(sinkCloseBasePath).exists());
            assertFalse(new File(sinkHighLowBasePath).exists());
        }
    }

}

