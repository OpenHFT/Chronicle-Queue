/*
 * Copyright 2013 Peter Lawrey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package vanilla.java.processingengine;

import net.openhft.affinity.AffinitySupport;
import net.openhft.chronicle.ChronicleConfig;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.IndexedChronicle;
import net.openhft.chronicle.tools.ChronicleTools;
import net.openhft.lang.model.constraints.NotNull;
import vanilla.java.processingengine.api.*;

import java.io.IOException;

/**
 * @author peter.lawrey
 */
public class PEMain {
    public static void main(String... ignored) throws IOException {
        ChronicleTools.warmup();

        AffinitySupport.setAffinity(1L << 2);
        String tmp = System.getProperty("java.io.tmpdir");
//        String tmp = System.getProperty("user.home");

//        ChronicleConfig config = ChronicleConfig.DEFAULT.clone();
//        config.dataBlockSize(4 * 1024);
//        config.indexBlockSize(4 * 1024);

        String pePath = tmp + "/demo/pe";
        ChronicleTools.deleteDirOnExit(pePath);
        IndexedChronicle pe2gw = new IndexedChronicle(pePath, ChronicleConfig.DEFAULT);
        ExcerptAppender excerpt = pe2gw.createAppender();
        final Pe2GwWriter pe2GwWriter = new Pe2GwWriter(excerpt);

        Gw2PeEvents listener = new PEEvents(pe2GwWriter);
        Gw2PeReader[] readers = new Gw2PeReader[1];
        IndexedChronicle[] gw2pe = new IndexedChronicle[readers.length];
        for (int i = 0; i < readers.length; i++) {
            int sourceId = i + 1;
            String gw2pePath = tmp + "/demo/gw2pe" + sourceId;
            ChronicleTools.deleteDirOnExit(gw2pePath);

            gw2pe[i] = new IndexedChronicle(gw2pePath, ChronicleConfig.DEFAULT);
            readers[i] = new Gw2PeReader(sourceId, gw2pe[i].createTailer(), listener);
        }

        long prevProcessed = 0, count = 0;
        //noinspection InfiniteLoopStatement
        do {
            boolean readOne = false;
            for (Gw2PeReader reader : readers) {
                readOne |= reader.readOne();
            }
            if (readOne) {
                // did something
                count = 0;
            } else if (count++ > 1000000) {
                // do something else like pause.
                long processed = excerpt.index() + 1;
                if (prevProcessed != processed) {
                    System.out.printf("Processed %,d requests%n", processed);
                    prevProcessed = processed;
                }
            }
        } while (true);
    }

    static class PEEvents implements Gw2PeEvents {
        private final Pe2GwWriter pe2GwWriter;
        private final SmallReport smallReport = new SmallReport();
//        int count = 0;

        public PEEvents(Pe2GwWriter pe2GwWriter) {
            this.pe2GwWriter = pe2GwWriter;
        }

        @Override
        public void small(@NotNull MetaData metaData, @NotNull SmallCommand command) {
            smallReport.orderOkay(command.clientOrderId);
//            System.out.println(++count);
            pe2GwWriter.report(metaData, smallReport);
        }
    }
}
