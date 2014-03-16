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

import net.openhft.chronicle.ExcerptAppender;
import net.openhft.lang.model.constraints.NotNull;

/**
 * @author peter.lawrey
 */
public class Pe2GwWriter implements Pe2GwEvents {
    private final ExcerptAppender excerpt;

    public Pe2GwWriter(ExcerptAppender excerpt) {
        this.excerpt = excerpt;
    }

    @Override
    public void report(@NotNull MetaData metaData, @NotNull SmallReport smallReport) {
        excerpt.startExcerpt();
        excerpt.writeEnum(MessageType.report);
        metaData.writeForEngine(excerpt);
        smallReport.writeMarshallable(excerpt);
        excerpt.finish();
    }
}
