/*
 * Copyright 2015 Higher Frequency Trading
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

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.IndexedChronicle;
import net.openhft.chronicle.VanillaChronicle;
import net.openhft.chronicle.tools.WrappedExcerptAppender;
import net.openhft.lang.model.constraints.NotNull;

import java.io.IOException;

public abstract class AppenderAdapter extends WrappedExcerptAppender<ExcerptAppender> {

    public AppenderAdapter(@NotNull ExcerptAppender appender) {
        super(appender);
    }

    public abstract void writePaddedEntry();
    public abstract void startExcerpt(long capacity, long index);
}
