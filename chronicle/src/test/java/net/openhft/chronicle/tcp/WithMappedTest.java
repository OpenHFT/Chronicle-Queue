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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;

import static org.junit.Assert.assertFalse;

/**
 * @author Rob Austin.
 */
public class WithMappedTest extends ChronicleTcpTestBase {

    @Rule
    public final TestName testName = new TestName();

    private static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");


    private static class PriceData implements BytesMarshallable {

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

            PriceData priceData = (PriceData) o;

            if (date != priceData.date) return false;

            return true;
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

    private static final MappingFunction MARKET_DATA_TO_PRICE_DATA_FILTER = new MappingFunction() {
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
    public void testReplicationWithPriceMarketDataFilter() throws Exception {

        final String sourceBasePath = getVanillaTestPath("-source");
        final String sinkBasePath = getVanillaTestPath("-sink");

        final ChronicleTcpTestBase.PortSupplier portSupplier = new ChronicleTcpTestBase.PortSupplier();

        final Chronicle source = ChronicleQueueBuilder.vanilla(sourceBasePath)
                .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
                .build();

        final int port = portSupplier.getAndCheckPort();

        final Chronicle sink = ChronicleQueueBuilder.vanilla(sinkBasePath)
                .sink()
                .withMapping(MARKET_DATA_TO_PRICE_DATA_FILTER) // this is sent to the source
                .connectAddress("localhost", port)
                .build();

        try {


            final Collection<MarketData> marketRecords = loadMarketData();

            final Thread at = new Thread("th-appender") {
                public void run() {
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
                }
            };

            final Thread tt = new Thread("th-tailer") {
                public void run() {
                    AffinityLock lock = AffinityLock.acquireLock();
                    try {
                        final ExcerptTailer tailer = sink.createTailer();


                        while (tailer.nextIndex()) {

                            PriceData priceData = new PriceData();
                            priceData.readMarshallable(tailer);

                            // check the data is reasonable
                            Assert.assertTrue(priceData.date > DATE_FORMAT.parse("2014-01-01").getTime());
                            Assert.assertTrue(priceData.date < DATE_FORMAT.parse("2016-01-01").getTime());


                            Assert.assertTrue(priceData.high > 5000);
                            Assert.assertTrue(priceData.high < 8000);

                            Assert.assertTrue(priceData.high > 5000);
                            Assert.assertTrue(priceData.high < 8000);

                            Assert.assertTrue(priceData.low < priceData.high);

                            tailer.finish();

                        }


                        tailer.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        lock.release();
                    }
                }
            };

            at.start();
            tt.start();

            at.join();
            tt.join();
        } finally {
            sink.close();
            sink.clear();

            source.close();
            source.clear();

            assertFalse(new File(sourceBasePath).exists());
            assertFalse(new File(sinkBasePath).exists());
        }
    }

}

