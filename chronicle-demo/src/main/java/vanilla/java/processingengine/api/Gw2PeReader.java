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

package vanilla.java.processingengine.api;

import net.openhft.chronicle.ExcerptTailer;

/**
 * @author peter.lawrey
 */
public class Gw2PeReader {
    private final int sourceId;
    private final ExcerptTailer excerpt;
    private final Gw2PeEvents peEvents;
    private final MetaData metaData;
    private final SmallCommand smallCommand = new SmallCommand();

    public Gw2PeReader(int sourceId, ExcerptTailer excerpt, Gw2PeEvents peEvents) {
        this.excerpt = excerpt;
        this.peEvents = peEvents;
        metaData = new MetaData(true);
        this.sourceId = sourceId;
    }

    public boolean readOne() {
        if (!excerpt.nextIndex()) {
//            System.out.println("r- " + excerpt.index());
            return false;
        }

        long pos = excerpt.position();
//        System.out.println("r " + excerpt.index() + " " + excerpt.capacity());
        MessageType mt = excerpt.readEnum(MessageType.class);
        if (mt == null) {
            // rewind and read again.
            excerpt.position(pos);
            System.err.println("Unknown message type " + excerpt.readUTF());
            return true;
        }
        switch (mt) {
            case small: {
                metaData.sourceId = sourceId;
                metaData.readFromGateway(excerpt);
                smallCommand.readMarshallable(excerpt);
                peEvents.small(metaData, smallCommand);
                break;
            }
            default:
                System.err.println("Unknown message type " + mt);
                break;
        }
        return true;
    }
}
