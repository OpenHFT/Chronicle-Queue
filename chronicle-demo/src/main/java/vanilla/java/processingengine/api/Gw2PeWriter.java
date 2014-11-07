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

package vanilla.java.processingengine.api;

import net.openhft.chronicle.ExcerptAppender;
import net.openhft.lang.model.constraints.NotNull;

/**
 * Gateway to Processing Engine writer
 *
 * @author peter.lawrey
 */
public class Gw2PeWriter implements Gw2PeEvents {
    private final ExcerptAppender excerpt;

    public Gw2PeWriter(ExcerptAppender excerpt) {
        this.excerpt = excerpt;
    }

    @Override
    public void small(MetaData ignored, @NotNull SmallCommand command) {
        excerpt.startExcerpt();
        excerpt.writeEnum(MessageType.small);
        MetaData.writeForGateway(excerpt);
        command.writeMarshallable(excerpt);
        excerpt.finish();
    }
}
