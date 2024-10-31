/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.impl.TableStore;
import org.jetbrains.annotations.NotNull;

import java.io.File;

class TableDirectoryListingReadOnly extends TableDirectoryListing {

    TableDirectoryListingReadOnly(final @NotNull TableStore<?> tableStore,
                                  final TimeProvider time) {
        super(tableStore, null, null, time);
    }

    @Override
    protected void checkReadOnly(@NotNull TableStore<?> tableStore) {
        // no-op
    }

    @Override
    public void init() {
        throwExceptionIfClosedInSetter();

        // it is possible if r/o queue created at same time as r/w queue for longValues to be only half-written
        final long timeoutMillis = System.currentTimeMillis() + 500;
        while (true) {
            try {
                initLongValues();
                break;
            } catch (Exception e) {
                if (System.currentTimeMillis() > timeoutMillis)
                    throw e;
                Jvm.pause(1);
            }
        }
    }

    @Override
    public void refresh(final boolean force) {
        // no-op
    }

    @Override
    public void onFileCreated(final File file, final int cycle) {
        onRoll(cycle);
    }

    @Override
    public void onRoll(int cycle) {
        // no-op
    }
}
